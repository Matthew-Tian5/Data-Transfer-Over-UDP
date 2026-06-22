#!/usr/bin/env python3
"""
receiver_gbn.py
===============

Go-Back-N receiver (CP372 Assignment 2, Part C).

Behaviour
---------
The receiver keeps two values:
    expected     : the sequence number it wants next (in-order)
    last_acked   : the highest sequence number delivered so far (-1 if none)

For each incoming packet (after the loss/corruption checks):

  * If seq == expected, it is the in-order packet. Deliver it (by TYPE),
    set last_acked = expected, send a CUMULATIVE ACK for it, advance expected.
  * Otherwise (out-of-order or duplicate), discard it and re-send the
    cumulative ACK for `last_acked` so the sender knows the highest in-order
    packet that arrived.

This matches the assignment example exactly:
    expected = 3, receive 4  ->  discard 4, send ACK 2 (= last_acked).

Because the receiver only ever accepts the next expected packet, packets are
always written to disk in order. As with Stop-and-Wait, the packet TYPE chooses
the action: START creates the file, DATA appends, FIN closes and verifies it.

Simulated loss (--loss) and corruption (--corruption) work the same way as in
the Stop-and-Wait receiver.

Usage examples
--------------
    python receiver_gbn.py --port 5005
    python receiver_gbn.py --loss 0.2 --verbose
    python receiver_gbn.py --loss 0.1 --corruption 0.05
"""

import argparse
import os

import rdt_common as rdt


def run(args):
    log = rdt.setup_logging("gbn-recv", args.verbose, args.quiet)

    sock = rdt.make_udp_socket(bind_addr=(args.host, args.port))
    os.makedirs(args.output_dir, exist_ok=True)
    rdt.signal_ready(args.ready_fd)

    log.info("Go-Back-N receiver listening on %s:%d "
             "[loss=%.0f%%, corruption=%.0f%%]",
             args.host, args.port, args.loss * 100, args.corruption * 100)

    expected = 0
    last_acked = -1       # highest in-order seq delivered (nothing yet)
    out_file = None
    meta = None
    bytes_written = 0
    dropped = 0
    corrupted = 0
    finished = False
    deadline = None

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

        # --- Bonus: corrupt, then let the checksum reject it ----------------
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

        if pkt.seq == expected:
            # Accept the next expected packet and deliver by TYPE.
            if pkt.ptype == rdt.TYPE_START:
                meta = rdt.parse_start_metadata(pkt.payload)
                out_path = os.path.join(args.output_dir, meta["filename"])
                out_file = open(out_path, "wb")
                bytes_written = 0
                log.info("START: receiving '%s' (%d bytes, %d data packets, window=%s)",
                         meta["filename"], meta["filesize"],
                         meta["num_data_packets"], meta.get("window"))

            elif pkt.ptype == rdt.TYPE_DATA:
                if out_file is not None:
                    out_file.write(pkt.payload)
                    bytes_written += len(pkt.payload)
                log.debug("DATA seq=%d (%d bytes) written", pkt.seq, len(pkt.payload))

            elif pkt.ptype == rdt.TYPE_FIN:
                if out_file is not None:
                    out_file.close()
                log.info("FIN: transfer complete (%d bytes written)", bytes_written)
                _verify(log, meta, args.output_dir, bytes_written)
                finished = True

            last_acked = expected
            sock.sendto(rdt.make_ack(last_acked), addr)
            log.debug("<- cumulative ACK %d", last_acked)
            expected += 1

            if finished and deadline is None:
                deadline = rdt.now() + args.linger

        else:
            # Out-of-order or duplicate: discard and re-ACK the last in-order seq.
            if last_acked >= 0:
                sock.sendto(rdt.make_ack(last_acked), addr)
                log.debug("   discarded %s (expected %d); re-ACK %d",
                          pkt, expected, last_acked)
            else:
                # Nothing delivered yet and this is not packet 0: just drop it.
                log.debug("   discarded %s (expected %d, nothing acked yet)",
                          pkt, expected)

    sock.close()
    log.info("Receiver done. simulated-losses=%d, corrupted-discards=%d", dropped, corrupted)
    return 0


def _verify(log, meta, output_dir, bytes_written):
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
    parser = argparse.ArgumentParser(description="Go-Back-N reliable receiver over UDP")
    rdt.add_common_net_args(parser, is_sender=False)
    rdt.add_receiver_args(parser)
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())