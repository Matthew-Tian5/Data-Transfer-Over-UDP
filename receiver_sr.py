#!/usr/bin/env python3
"""
receiver_sr.py
==============

Selective Repeat receiver (CP372 Assignment 2, Bonus Option A).

How it differs from Go-Back-N
-----------------------------
The big idea behind Selective Repeat is that the receiver does NOT throw away
correctly received packets just because an earlier one is missing. Instead it
buffers any packet that falls inside its receive window and acknowledges each
one individually. Once the missing packet finally shows up, a whole run of
buffered packets can be delivered to the file in order at once.

State
-----
    rcv_base   : sequence number of the oldest packet not yet delivered
    window     : size of the receive window (packets in [rcv_base, rcv_base+window)
                 may be accepted and buffered)
    buffer     : seq -> (ptype, payload) for packets received but not yet delivered

For each incoming packet (after the loss / corruption checks):

  * If it is inside the window [rcv_base, rcv_base + window):
        - send an individual ACK for that exact sequence number,
        - buffer it (if not already buffered),
        - if it just filled the rcv_base slot, deliver rcv_base and every
          buffered packet directly after it, sliding rcv_base forward.
  * If it is just below the window [rcv_base - window, rcv_base): it was already
        delivered but our ACK was lost, so re-ACK that exact sequence number.
  * Anything else is ignored.

Because packets are only ever DELIVERED in strict sequence order, the file is
always written in order. As elsewhere, the packet TYPE chooses the action:
START creates the file, DATA appends, FIN closes and verifies it.

ACKs here are INDIVIDUAL (ACK = the seq of the one packet received), which is
the key wire-level difference from Go-Back-N's cumulative ACKs.

Simulated loss (--loss) and corruption (--corruption) behave exactly as in the
other receivers.

Usage examples
--------------
    python receiver_sr.py --port 5005 --window 16
    python receiver_sr.py --loss 0.2 --window 16 --verbose
    python receiver_sr.py --loss 0.1 --corruption 0.05
"""

import argparse
import os

import rdt_common as rdt


def run(args):
    log = rdt.setup_logging("sr-recv", args.verbose, args.quiet)

    sock = rdt.make_udp_socket(bind_addr=(args.host, args.port))
    os.makedirs(args.output_dir, exist_ok=True)
    rdt.signal_ready(args.ready_fd)

    window = max(1, args.window)
    log.info("Selective Repeat receiver listening on %s:%d "
             "[window=%d, loss=%.0f%%, corruption=%.0f%%]",
             args.host, args.port, window, args.loss * 100, args.corruption * 100)

    rcv_base = 0
    buffer = {}           # seq -> (ptype, payload) held until deliverable
    out_file = None
    meta = None
    bytes_written = 0
    dropped = 0
    corrupted = 0
    finished = False
    deadline = None       # linger window after FIN, for re-acking duplicates

    def deliver(ptype, payload):
        """Act on one in-order packet according to its TYPE."""
        nonlocal out_file, meta, bytes_written, finished, deadline, window
        if ptype == rdt.TYPE_START:
            meta = rdt.parse_start_metadata(payload)
            out_path = os.path.join(args.output_dir, meta["filename"])
            out_file = open(out_path, "wb")
            bytes_written = 0
            # Make sure our window is at least as large as the sender's.
            window = max(window, int(meta.get("window") or window))
            log.info("START: receiving '%s' (%d bytes, %d data packets, window=%d)",
                     meta["filename"], meta["filesize"],
                     meta["num_data_packets"], window)
        elif ptype == rdt.TYPE_DATA:
            if out_file is not None:
                out_file.write(payload)
                bytes_written += len(payload)
        elif ptype == rdt.TYPE_FIN:
            if out_file is not None:
                out_file.close()
            log.info("FIN: transfer complete (%d bytes written)", bytes_written)
            _verify(log, meta, args.output_dir, bytes_written)
            finished = True
            if deadline is None:
                deadline = rdt.now() + args.linger

    while True:
        if deadline is not None:
            wait = deadline - rdt.now()
            if wait <= 0:
                break
            sock.settimeout(wait)
        else:
            sock.settimeout(args.idle_timeout)

        try:
            raw, addr = sock.recvfrom(rdt.RECV_BUFFER)
        except OSError:
            if finished:
                break
            log.warning("No packets for %.1fs, giving up.", args.idle_timeout)
            break

        # --- Bonus: corrupt the bytes, then let the checksum reject them -----
        raw, _ = rdt.maybe_corrupt(raw, args.corruption)

        pkt = rdt.Packet.decode(raw)
        if pkt is None:
            corrupted += 1
            log.debug("   discarded corrupted/invalid datagram (checksum fail)")
            continue

        # --- Simulated packet loss -----------------------------------------
        if rdt.should_drop(args.loss):
            dropped += 1
            log.debug("   (simulated) dropped %s", pkt)
            continue

        if pkt.ptype == rdt.TYPE_ACK:
            continue

        s = pkt.seq

        if rcv_base <= s < rcv_base + window:
            # Inside the window: ACK it individually and buffer it.
            sock.sendto(rdt.make_ack(s), addr)
            if s not in buffer:
                buffer[s] = (pkt.ptype, pkt.payload)
                log.debug("buffered seq=%d, ACK %d", s, s)
            else:
                log.debug("duplicate seq=%d (already buffered), re-ACK %d", s, s)

            # If we just filled the base slot, flush the contiguous run.
            while rcv_base in buffer:
                ptype, payload = buffer.pop(rcv_base)
                deliver(ptype, payload)
                rcv_base += 1

        elif rcv_base - window <= s < rcv_base:
            # Already delivered earlier; the sender thinks it was lost. Re-ACK.
            sock.sendto(rdt.make_ack(s), addr)
            log.debug("re-ACK %d (already delivered)", s)

        else:
            log.debug("   ignored out-of-window %s (rcv_base=%d)", pkt, rcv_base)

    sock.close()
    log.info("Receiver done. simulated-losses=%d, corrupted-discards=%d", dropped, corrupted)
    return 0


def _verify(log, meta, output_dir, bytes_written):
    """Check the received file against the size and SHA-256 from the sender."""
    if not meta:
        return
    out_path = os.path.join(output_dir, meta["filename"])
    size_ok = bytes_written == meta["filesize"]
    try:
        got_hash = rdt.sha256_of_file(out_path)
    except OSError:
        got_hash = None
    hash_ok = got_hash == meta.get("sha256")
    if size_ok and hash_ok:
        log.info("INTEGRITY OK: size and SHA-256 match the original.")
    else:
        log.warning("INTEGRITY MISMATCH: size_ok=%s hash_ok=%s", size_ok, hash_ok)


def main():
    parser = argparse.ArgumentParser(description="Selective Repeat reliable receiver over UDP")
    rdt.add_common_net_args(parser, is_sender=False)
    rdt.add_receiver_args(parser)
    parser.add_argument("--window", type=int, default=rdt.DEFAULT_WINDOW,
                        help="receive window size (should be >= sender window; "
                             "default: %(default)s)")
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())