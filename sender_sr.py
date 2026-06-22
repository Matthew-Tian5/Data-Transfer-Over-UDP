#!/usr/bin/env python3
"""
sender_sr.py
============

Selective Repeat sender (CP372 Assignment 2, Bonus Option A).

This implementation uses a SINGLE timer (the bonus requirement), not one timer
per packet. The single timer always tracks the oldest unacknowledged packet
(`base`). When it fires, only the packets in the window that are still unacked
are retransmitted: packets the receiver has already acknowledged are skipped.
That "skip the acked ones" behaviour is what makes this Selective Repeat rather
than Go-Back-N, where the entire window is resent.

Sliding window state
--------------------
    base         : sequence number of the oldest unacknowledged packet
    nextseqnum   : sequence number of the next packet to send
    window       : how many packets may be outstanding at once
    acked[s]     : True once an individual ACK for sequence s has arrived

Packets share one sequence space:  START (0), DATA (1..N), FIN (N+1).

Behaviour
---------
  * Send packets back-to-back while nextseqnum < base + window (and packets
    remain), without waiting for ACKs.
  * ACKs are INDIVIDUAL: an ACK for sequence `a` marks only packet `a` as
    received. The window slides only when `base` itself becomes acked, at which
    point base jumps forward over every consecutively-acked packet.
  * Run ONE timer for the current base packet. On timeout, retransmit only the
    still-unacked packets inside [base, nextseqnum) and restart the timer.

Like the other senders this is a single-threaded select() loop, so there are no
locks and no race conditions.

Usage examples
--------------
    python sender_sr.py --file myfile.txt --window 16
    python sender_sr.py --size 1M --window 16 --verbose
    python sender_sr.py --file myfile.txt --window 16 --json
"""

import argparse
import os
import select
import sys
import tempfile

import rdt_common as rdt


def run(args):
    log = rdt.setup_logging("sr-send", args.verbose, args.quiet)

    # --- Resolve the source file -------------------------------------------
    generated = None
    if args.size is not None:
        fd, path = tempfile.mkstemp(prefix="sr_send_", suffix=".bin")
        os.close(fd)
        rdt.make_random_file(path, args.size)
        generated = path
        filepath = path
        log.info("Generated %d-byte random test file", args.size)
    else:
        filepath = args.file
        if not os.path.isfile(filepath):
            log.error("File not found: %s", filepath)
            return 1

    filesize = os.path.getsize(filepath)
    mss = max(1, min(args.mss, rdt.MAX_MSS))
    window = max(1, args.window)
    file_hash = rdt.sha256_of_file(filepath)

    # --- Build the ordered packet list -------------------------------------
    data_chunks = rdt.chunk_file(filepath, mss)
    num_data = len(data_chunks)
    meta = rdt.build_start_metadata(
        filepath, filesize, mss, num_data, file_hash, "sr", window=window)

    packets = [rdt.Packet(rdt.TYPE_START, seq=0, payload=meta)]
    for i, chunk in enumerate(data_chunks, start=1):
        packets.append(rdt.Packet(rdt.TYPE_DATA, seq=i, payload=chunk))
    packets.append(rdt.Packet(rdt.TYPE_FIN, seq=num_data + 1))
    total = len(packets)

    # Pre-encode every packet once so the send loop stays cheap.
    wire = [p.encode() for p in packets]

    # --- Socket + timer ----------------------------------------------------
    sock = rdt.make_udp_socket()
    dest = (args.host, args.port)
    rtt = rdt.RttEstimator()
    if args.timeout is not None:
        rtt.use_fixed(args.timeout)

    log.info("Sending '%s' (%d bytes) to %s:%d via Selective Repeat "
             "[mss=%d, window=%d, %d data packets]",
             os.path.basename(filepath), filesize, args.host, args.port,
             mss, window, num_data)

    base = 0
    nextseqnum = 0
    acked = [False] * total
    send_time = [0.0] * total      # per-packet send time, for RTT sampling
    retransmitted = [False] * total
    retransmissions = 0
    packets_sent = 0
    timer_start = None             # None => timer not running
    base_attempts = 0              # retransmit count for the current base

    start_time = rdt.now()

    while base < total:
        # --- Fill the window ------------------------------------------------
        while nextseqnum < base + window and nextseqnum < total:
            sock.sendto(wire[nextseqnum], dest)
            send_time[nextseqnum] = rdt.now()
            packets_sent += 1
            log.debug("-> sent %s", packets[nextseqnum])
            if base == nextseqnum:
                # Oldest unacked packet just went out: (re)start the timer.
                timer_start = rdt.now()
                base_attempts = 0
            nextseqnum += 1

        # --- Wait for an ACK or for the timer to expire ---------------------
        if timer_start is None:
            timer_start = rdt.now()  # defensive; base<total implies in-flight data
        remaining = rtt.timeout() - (rdt.now() - timer_start)
        ready = select.select([sock], [], [], remaining)[0] if remaining > 0 else []

        if not ready:
            # ---- Timeout: retransmit only the UNACKED packets in the window.
            base_attempts += 1
            if base_attempts > args.max_retries:
                log.error("Gave up on base=%d after %d retries", base, base_attempts)
                sock.close()
                return 2
            resent = 0
            for s in range(base, nextseqnum):
                if not acked[s]:
                    sock.sendto(wire[s], dest)
                    retransmitted[s] = True
                    packets_sent += 1
                    retransmissions += 1
                    resent += 1
            log.debug("   timeout: resent %d unacked packet(s) in [%d, %d)",
                      resent, base, nextseqnum)
            timer_start = rdt.now()
            continue

        # ---- An ACK (or some datagram) arrived -----------------------------
        raw, _ = sock.recvfrom(rdt.RECV_BUFFER)

        if rdt.should_drop(args.loss):
            log.debug("   (simulated) dropped an incoming ACK")
            continue

        ack = rdt.Packet.decode(raw)
        if ack is None or ack.ptype != rdt.TYPE_ACK:
            continue

        a = ack.ack
        if base <= a < nextseqnum and not acked[a]:
            # First individual ACK for packet a.
            if not retransmitted[a]:
                rtt.update(rdt.now() - send_time[a])  # Karn: only un-retransmitted
            acked[a] = True
            log.debug("<- ACK %d", a)

            if a == base:
                # Slide base forward over every consecutively-acked packet.
                while base < total and acked[base]:
                    base += 1
                if base == nextseqnum:
                    timer_start = None        # nothing in flight: stop the timer
                else:
                    timer_start = rdt.now()   # restart timer for the new base
                    base_attempts = 0
            # An ACK for a non-base packet just gets recorded; base stays put
            # and the single timer keeps running for the current base.
        else:
            log.debug("<- stale/duplicate ACK %d (base=%d)", a, base)

    elapsed = rdt.now() - start_time
    sock.close()

    throughput = filesize / elapsed if elapsed > 0 else 0.0
    _report(args, log, "sr", filepath, filesize, mss, window,
            num_data, packets_sent, retransmissions, elapsed, throughput)

    if generated and os.path.exists(generated):
        os.remove(generated)
    return 0


def _report(args, log, protocol, filepath, filesize, mss, window,
            num_data, packets_sent, retransmissions, elapsed, throughput):
    log.info("Transfer complete in %.4fs", elapsed)
    log.info("  file size       : %d bytes", filesize)
    log.info("  data packets    : %d", num_data)
    log.info("  window size     : %d", window)
    log.info("  packets sent    : %d (incl. retransmissions)", packets_sent)
    log.info("  retransmissions : %d", retransmissions)
    log.info("  throughput      : %.2f bytes/sec", throughput)
    if args.json:
        import json
        print(json.dumps({
            "protocol": protocol,
            "file": os.path.basename(filepath),
            "file_size": filesize,
            "mss": mss,
            "window": window,
            "loss_rate": args.loss,
            "data_packets": num_data,
            "packets_sent": packets_sent,
            "retransmissions": retransmissions,
            "transfer_time": round(elapsed, 6),
            "throughput": round(throughput, 3),
        }))
        sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="Selective Repeat reliable sender over UDP")
    rdt.add_common_net_args(parser, is_sender=True)
    rdt.add_sender_io_args(parser)
    parser.add_argument("--window", type=int, default=rdt.DEFAULT_WINDOW,
                        help="sliding window size (default: %(default)s)")
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())