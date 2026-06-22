"""
rdt_common.py
=============

Shared building blocks for the CP372 Assignment 2 reliable data transfer
protocols (Stop-and-Wait, Go-Back-N, and the Selective Repeat bonus).

Everything that the senders and receivers have in common lives here so the
protocol files stay focused on their own logic:

  * The on-the-wire Packet format (a fixed binary header + payload).
  * A 16-bit Internet checksum for error detection (used by the bonus task).
  * UDP socket creation with sensible buffer sizes.
  * An adaptive RTT / timeout estimator (Jacobson/Karn style).
  * Helpers for simulating packet loss and packet corruption.
  * Metadata helpers for the START packet (filename, size, hash, etc.).
  * Small shared CLI argument helpers and a logging setup.

All four protocols share ONE sequence space. Control packets are numbered
right alongside the data:

    seq 0            -> START  ("upload a file", carries JSON metadata)
    seq 1 .. N       -> DATA   (the file payload, one chunk per packet)
    seq N+1          -> FIN     ("completion of file transfer")

Delivering packets in order therefore naturally runs the receiver through
open-file -> write-chunks -> close-file without any special casing.

Sequence numbers are absolute 32-bit values and never wrap for any file size
in this assignment (a 4 GB sequence space at >= 1 byte per packet covers up to
billions of packets), which keeps the sliding-window arithmetic simple and
bug-free.
"""
import argparse
import hashlib
import json
import logging
import os
import random
import socket
import struct
import time

# ---------------------------------------------------------------------------
# Packet types
# ---------------------------------------------------------------------------
TYPE_START = 1   # "upload a file": receiver creates the output file
TYPE_DATA = 2    # DATA: receiver writes the payload to the file
TYPE_FIN = 3     # "completion of file transfer": receiver closes the file
TYPE_ACK = 4     # acknowledgment (the acked sequence number is in the ack field)

TYPE_NAMES = {
    TYPE_START: "START",
    TYPE_DATA: "DATA",
    TYPE_FIN: "FIN",
    TYPE_ACK: "ACK",
}

# ---------------------------------------------------------------------------
# Packet header layout (network byte order, no padding because of '!')
#
#   ptype     : 1 byte  (unsigned char)   one of the TYPE_* constants
#   seq       : 4 bytes (unsigned int)    sequence number
#   ack       : 4 bytes (unsigned int)    acknowledgment number
#   checksum  : 2 bytes (unsigned short)  16-bit Internet checksum
#   length    : 2 bytes (unsigned short)  payload length in bytes
#
# Total header size = 13 bytes, followed by `length` payload bytes.
# ---------------------------------------------------------------------------
HEADER_FMT = "!BIIHH"
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 13 bytes

# Largest UDP datagram we will ever read. Big enough for any MSS we allow.
RECV_BUFFER = 65535

# Defaults shared across the programs.
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5005
DEFAULT_MSS = 1024            # payload bytes per DATA packet
DEFAULT_WINDOW = 16           # window size for GBN / SR
MAX_MSS = RECV_BUFFER - HEADER_SIZE


# ===========================================================================
# Internet checksum (16-bit one's complement) - used for the corruption bonus
# ===========================================================================
def internet_checksum(data: bytes) -> int:
    """Return the 16-bit one's-complement Internet checksum of `data`.

    This is the same algorithm used by IP/UDP/TCP. We unpack the buffer into
    big-endian 16-bit words in a single C-level call and sum them, which keeps
    it fast enough to run on every packet even for large files.
    """
    if len(data) & 1:                      # pad to an even number of bytes
        data += b"\x00"
    words = struct.unpack("!%dH" % (len(data) // 2), data)
    total = sum(words)
    # Fold the 32-bit running sum back into 16 bits (twice covers any carry).
    total = (total & 0xFFFF) + (total >> 16)
    total = (total & 0xFFFF) + (total >> 16)
    return (~total) & 0xFFFF


# ===========================================================================
# Packet: encode / decode the binary wire format
# ===========================================================================
class Packet:
    """A single RDT packet. Use encode()/decode() to go to and from bytes."""

    __slots__ = ("ptype", "seq", "ack", "payload")

    def __init__(self, ptype: int, seq: int = 0, ack: int = 0, payload: bytes = b""):
        self.ptype = ptype
        self.seq = seq
        self.ack = ack
        self.payload = payload

    def encode(self) -> bytes:
        """Serialise the packet to bytes, filling in the checksum field."""
        length = len(self.payload)
        # Build the header once with a zero checksum, compute over header+payload,
        # then rebuild the header with the real checksum in place.
        header_zero = struct.pack(HEADER_FMT, self.ptype, self.seq, self.ack, 0, length)
        chk = internet_checksum(header_zero + self.payload)
        header = struct.pack(HEADER_FMT, self.ptype, self.seq, self.ack, chk, length)
        return header + self.payload

    @classmethod
    def decode(cls, raw: bytes):
        """Parse bytes into a Packet.

        Returns None if the datagram is malformed OR fails the checksum, so the
        caller can treat a corrupted packet exactly like a lost one (discard it).
        """
        if len(raw) < HEADER_SIZE:
            return None
        ptype, seq, ack, chk, length = struct.unpack(HEADER_FMT, raw[:HEADER_SIZE])
        payload = raw[HEADER_SIZE:]
        # Length sanity check catches corruption that lands in the length field.
        if len(payload) != length:
            return None
        # Verify the checksum (recompute with the checksum field zeroed).
        header_zero = struct.pack(HEADER_FMT, ptype, seq, ack, 0, length)
        if internet_checksum(header_zero + payload) != chk:
            return None
        return cls(ptype, seq, ack, payload)

    @property
    def type_name(self) -> str:
        return TYPE_NAMES.get(self.ptype, "?")

    def __repr__(self) -> str:
        return "Packet(%s seq=%d ack=%d len=%d)" % (
            self.type_name, self.seq, self.ack, len(self.payload))


# Convenience constructors --------------------------------------------------
def make_ack(ack_num: int) -> bytes:
    """Encode an ACK packet acknowledging sequence number `ack_num`."""
    return Packet(TYPE_ACK, seq=0, ack=ack_num).encode()


# ===========================================================================
# UDP socket helper
# ===========================================================================
def make_udp_socket(bind_addr=None, bufsize=4 * 1024 * 1024) -> socket.socket:
    """Create a UDP socket with enlarged send/receive buffers.

    Larger kernel buffers stop the OS from silently dropping bursts of packets
    when the window is wide, so the only losses we see are the ones we simulate.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
        try:
            sock.setsockopt(socket.SOL_SOCKET, opt, bufsize)
        except OSError:
            pass  # not fatal if the OS clamps the request
    if bind_addr is not None:
        sock.bind(bind_addr)
    return sock


# ===========================================================================
# Adaptive timeout (RTT estimation, Jacobson/Karn)
# ===========================================================================
class RttEstimator:
    """Estimate a retransmission timeout from observed round-trip times.

    Uses the standard TCP smoothing:
        EstimatedRTT = (1 - alpha) * EstimatedRTT + alpha * SampleRTT
        DevRTT       = (1 - beta)  * DevRTT       + beta  * |SampleRTT - EstimatedRTT|
        Timeout      = EstimatedRTT + 4 * DevRTT      (clamped to [min, max])

    Per Karn's algorithm, callers must NOT feed in samples taken from packets
    that were retransmitted, because the RTT would be ambiguous.
    """

    def __init__(self, initial=0.05, alpha=0.125, beta=0.25,
                 min_timeout=0.01, max_timeout=2.0):
        self.estimated = initial
        self.dev = initial / 2.0
        self.alpha = alpha
        self.beta = beta
        self.min_timeout = min_timeout
        self.max_timeout = max_timeout
        self._fixed = None  # if set, timeout() always returns this value

    def use_fixed(self, value: float):
        """Force a fixed timeout (used when the user passes --timeout)."""
        self._fixed = value

    def update(self, sample_rtt: float):
        if self._fixed is not None:
            return
        self.estimated = (1 - self.alpha) * self.estimated + self.alpha * sample_rtt
        self.dev = (1 - self.beta) * self.dev + self.beta * abs(sample_rtt - self.estimated)

    def timeout(self) -> float:
        if self._fixed is not None:
            return self._fixed
        value = self.estimated + 4 * self.dev
        return min(self.max_timeout, max(self.min_timeout, value))


# ===========================================================================
# Loss / corruption simulation
# ===========================================================================
def should_drop(loss_rate: float) -> bool:
    """Return True with probability `loss_rate` (simulated packet loss)."""
    return loss_rate > 0.0 and random.random() < loss_rate


def maybe_corrupt(raw: bytes, corruption_rate: float):
    """With probability `corruption_rate`, flip one random bit in `raw`.

    Returns (possibly_modified_bytes, was_corrupted). A single bit flip is
    enough for the Internet checksum to reject the packet on decode, which is
    exactly what the corruption bonus is meant to demonstrate.
    """
    if corruption_rate > 0.0 and random.random() < corruption_rate and raw:
        idx = random.randrange(len(raw))
        bit = 1 << random.randrange(8)
        mutated = bytearray(raw)
        mutated[idx] ^= bit
        return bytes(mutated), True
    return raw, False


# ===========================================================================
# File / metadata helpers
# ===========================================================================
def sha256_of_file(path: str) -> str:
    """Return the hex SHA-256 of a file, read in chunks (fast, low memory)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_start_metadata(filename, filesize, mss, num_data_packets,
                         sha256, protocol, window=None) -> bytes:
    """Build the JSON payload carried by the START packet."""
    meta = {
        "filename": os.path.basename(filename),
        "filesize": filesize,
        "mss": mss,
        "num_data_packets": num_data_packets,
        "sha256": sha256,
        "protocol": protocol,
    }
    if window is not None:
        meta["window"] = window
    return json.dumps(meta).encode("utf-8")


def parse_start_metadata(payload: bytes) -> dict:
    """Parse the JSON metadata from a START packet payload."""
    return json.loads(payload.decode("utf-8"))


def chunk_file(path: str, mss: int):
    """Yield the file at `path` as a list of `mss`-byte chunks (last may be short)."""
    chunks = []
    with open(path, "rb") as f:
        while True:
            data = f.read(mss)
            if not data:
                break
            chunks.append(data)
    return chunks


def make_random_file(path: str, size_bytes: int):
    """Create a file of `size_bytes` pseudo-random bytes (for self-generated tests)."""
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    remaining = size_bytes
    with open(path, "wb") as f:
        while remaining > 0:
            block = min(remaining, 1 << 20)
            f.write(os.urandom(block))
            remaining -= block
    return path


# ===========================================================================
# Logging
# ===========================================================================
def setup_logging(name: str, verbose: bool, quiet: bool) -> logging.Logger:
    """Return a configured logger. Per-packet logs use DEBUG (verbose only)."""
    if quiet:
        level = logging.WARNING
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()  # logs go to stderr, keeping stdout clean
        handler.setFormatter(logging.Formatter("[%(name)s] %(message)s"))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


# ===========================================================================
# Shared CLI argument helpers
# ===========================================================================
def add_common_net_args(parser: argparse.ArgumentParser, is_sender: bool):
    """Add host/port/logging arguments common to every program."""
    if is_sender:
        parser.add_argument("--host", default=DEFAULT_HOST,
                            help="receiver host/IP (default: %(default)s)")
    else:
        parser.add_argument("--host", default=DEFAULT_HOST,
                            help="local bind address (default: %(default)s)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="UDP port (default: %(default)s)")
    parser.add_argument("--verbose", action="store_true",
                        help="log every packet (DEBUG level)")
    parser.add_argument("--quiet", action="store_true",
                        help="only log warnings and errors")


def add_sender_io_args(parser: argparse.ArgumentParser):
    """Add the file-selection and tuning arguments shared by the senders."""
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--file", help="path to the file to send")
    src.add_argument("--size", type=parse_size,
                     help="generate a random temp file of this size instead "
                          "(e.g. 10K, 500K, 1M, 100M)")
    parser.add_argument("--mss", type=int, default=DEFAULT_MSS,
                        help="payload bytes per DATA packet (default: %(default)s)")
    parser.add_argument("--timeout", type=float, default=None,
                        help="fixed retransmission timeout in seconds; "
                             "omit to use adaptive RTT-based timeout")
    parser.add_argument("--loss", type=float, default=0.0,
                        help="probability of dropping an INCOMING ACK at the "
                             "sender, to simulate reverse-path loss (default: 0)")
    parser.add_argument("--json", action="store_true",
                        help="print a single machine-readable JSON metrics line "
                             "on stdout (used by run_experiments.py)")
    parser.add_argument("--max-retries", type=int, default=10000,
                        help="safety cap on retransmissions of a single packet "
                             "before giving up (default: %(default)s)")


def add_receiver_args(parser: argparse.ArgumentParser):
    """Add the loss/corruption and output arguments shared by the receivers."""
    parser.add_argument("--loss", type=float, default=0.0,
                        help="probability of dropping each incoming DATA packet "
                             "(0.0-1.0), i.e. the simulated loss rate")
    parser.add_argument("--corruption", type=float, default=0.0,
                        help="probability of corrupting each incoming packet "
                             "(bonus task); corrupted packets fail the checksum")
    parser.add_argument("--output-dir", default="received_files",
                        help="directory to write received files into "
                             "(default: %(default)s)")
    parser.add_argument("--linger", type=float, default=1.0,
                        help="seconds to keep re-acking duplicates after the FIN "
                             "(TIME_WAIT style) before exiting (default: %(default)s)")
    parser.add_argument("--idle-timeout", type=float, default=30.0,
                        help="exit if no packet arrives for this many seconds, "
                             "so a dead transfer never hangs (default: %(default)s)")
    parser.add_argument("--ready-fd", type=int, default=None,
                        help="internal: write 'READY' to this fd once bound "
                             "(used by run_experiments.py to synchronise)")


def parse_size(text: str) -> int:
    """Parse a human size like '10K', '500K', '1M', '100M', '2G' into bytes."""
    text = text.strip().upper()
    units = {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "B": 1}
    if text and text[-1] in units:
        return int(float(text[:-1]) * units[text[-1]])
    return int(text)


def signal_ready(ready_fd):
    """Tell a parent orchestrator we are bound and ready to receive."""
    if ready_fd is None:
        return
    try:
        os.write(ready_fd, b"READY\n")
    except OSError:
        pass


def now() -> float:
    """Monotonic clock for timing (immune to wall-clock adjustments)."""
    return time.monotonic()