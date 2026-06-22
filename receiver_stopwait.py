#!/usr/bin/env python3
"""
receiver_stopwait.py
====================

Stop-and-Wait receiver (CP372 Assignment 2, Parts A and B).

Behaviour
---------
The receiver tracks a single value, `expected`, the sequence number of the
next packet it wants. For each incoming packet (after the loss/corruption
checks):

  * If it is the expected packet, the receiver delivers it (acting on the
    packet TYPE), sends an ACK for it, and advances `expected`.
  * If it is an older/duplicate packet (its ACK was probably lost), the
    receiver re-sends the ACK so the sender can make progress.
  * If it is a future/out-of-order packet, the receiver discards it.

Packet TYPE drives what "deliver" means:
    START -> create the output file (and read its metadata)
    DATA  -> append the payload to the file
    FIN   -> close the file and verify it against the sender's SHA-256

Simulated packet loss (Part B)
------------------------------
Before processing each decoded DATA/START/FIN packet, the receiver drops it
with probability --loss. A dropped packet is never acked, so the sender's
timer eventually fires and it retransmits: the file still arrives correctly.
Try --loss 0.0, 0.1, 0.2, 0.3.

Corruption detection (bonus)
----------------------------
With --corruption > 0, each incoming datagram has a random bit flipped before
decoding. The 16-bit Internet checksum then rejects it (decode returns None),
so it is discarded exactly like a lost packet.

Usage examples
--------------
    python receiver_stopwait.py
    python receiver_stopwait.py --port 5005 --loss 0.2 --verbose
    python receiver_stopwait.py --loss 0.1 --corruption 0.05
"""

import argparse
import os

import rdt_common as rdt


def run(args):
    log = rdt.setup_logging("sw-recv", args.verbose, args.quiet)

    sock = rdt.make_udp_socket(bind_addr=(args.host, args.port))
    os.makedirs(args.output_dir, exist_ok=True)
    rdt.signal_ready(args.ready_fd)

    log.info("Stop-and-Wait receiver listening on %s:%d "
             "[loss=%.0f%%, corruption=%.0f%%]",
             args.host, args.port, args.loss * 100, args.corruption * 100)

    expected = 0          # sequence number of the next in-order packet
    out_file = None       # open file handle for the transfer
    meta = None           # START metadata
    bytes_written = 0
    dropped = 0           # count of simulated losses
    corrupted = 0         # count of checksum failures
    finished = False
    deadline = None       # set after FIN: linger window for re-acking

    while True:
        # Bound the blocking recv so we can honour linger / idle timeouts.
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
            # Timed out. After FIN this is the normal way to exit; before FIN
            # it means the transfer stalled (sender gone), so we stop too.
            if finished:
                break
            log.warning("No packets for %.1fs, giving up.", args.idle_timeout)
            break

        # --- Bonus: corrupt the bytes, then let the checksum catch it -------
        raw, was_corrupted = rdt.maybe_corrupt(raw, args.corruption)

        pkt = rdt.Packet.decode(raw)
        if pkt is None:
            corrupted += 1
            log.debug("   discarded a corrupted/invalid datagram (checksum fail)")
            continue

        # --- Part B: simulated packet loss ---------------------------------
        if rdt.should_drop(args.loss):
            dropped += 1
            log.debug("   (simulated) dropped %s", pkt)
            continue

        if pkt.ptype == rdt.TYPE_ACK:
            continue  # receiver does not expect ACKs

        if pkt.seq == expected:
            # In-order packet: deliver it according to its type.
            if pkt.ptype == rdt.TYPE_START:
                meta = rdt.parse_start_metadata(pkt.payload)
                out_path = os.path.join(args.output_dir, meta["filename"])
                out_file = open(out_path, "wb")
                bytes_written = 0
                log.info("START: receiving '%s' (%d bytes, %d data packets)",
                         meta["filename"], meta["filesize"], meta["num_data_packets"])

            elif pkt.ptype == rdt.TYPE_DATA:
                if out_file is not None:
                    out_file.write(pkt.payload)
                    bytes_written += len(pkt.payload)
                log.debug("DATA seq=%d (%d bytes) written", pkt.seq, len(pkt.payload))

            elif pkt.ptype == rdt.TYPE_FIN:
                if out_file is not None:
                    out_file.close()
                log.info("FIN: file transfer complete (%d bytes written)", bytes_written)
                _verify(log, meta, args.output_dir, bytes_written)
                finished = True

            # ACK the packet we just accepted, then expect the next one.
            sock.sendto(rdt.make_ack(expected), addr)
            log.debug("<- ACK %d", expected)
            expected += 1

            if finished and deadline is None:
                # Enter the linger window: keep re-acking duplicates briefly.
                deadline = rdt.now() + args.linger

        elif pkt.seq < expected:
            # Duplicate (a previous ACK was lost). Re-ACK so the sender advances.
            sock.sendto(rdt.make_ack(pkt.seq), addr)
            log.debug("<- re-ACK %d (duplicate)", pkt.seq)

        else:
            # Future packet (out of order for Stop-and-Wait): discard it.
            log.debug("   discarded out-of-order %s (expected %d)", pkt, expected)

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
    parser = argparse.ArgumentParser(description="Stop-and-Wait reliable receiver over UDP")
    rdt.add_common_net_args(parser, is_sender=False)
    rdt.add_receiver_args(parser)
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())