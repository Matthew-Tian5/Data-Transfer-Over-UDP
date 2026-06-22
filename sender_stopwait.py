#!/usr/bin/env python3
"""
sender_stopwait.py
==================

Stop-and-Wait sender (CP372 Assignment 2, Part A).

Behaviour
---------
The whole transfer is a single ordered list of packets that share one
sequence space:

    START (seq 0)  ->  DATA (seq 1..N)  ->  FIN (seq N+1)

For each packet, the sender:
  1. sends the packet and starts a timer,
  2. waits for the matching ACK,
  3. retransmits the same packet if the timer expires,
  4. only advances to the next packet once the correct ACK arrives.

So exactly one packet is in flight at any moment, which is the defining
property of Stop-and-Wait.

The timer is adaptive by default (RTT-based, Karn's algorithm) so the
protocol stays fast on a local link and still behaves sensibly if delays grow.
Pass --timeout to force a fixed value instead.

Usage examples
--------------
    python sender_stopwait.py --file myfile.txt
    python sender_stopwait.py --size 1M --host 127.0.0.1 --port 5005 --verbose
    python sender_stopwait.py --file myfile.txt --json     # metrics for scripts
"""

import argparse
import os
import select
import sys
import tempfile

import rdt_common as rdt


def run(args):
    log = rdt.setup_logging("sw-send", args.verbose, args.quiet)

    # --- Resolve the source file (either a real file or a generated one) ----
    generated = None
    if args.size is not None:
        fd, path = tempfile.mkstemp(prefix="sw_send_", suffix=".bin")
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
    file_hash = rdt.sha256_of_file(filepath)

    # --- Build the full ordered packet list (START, DATA..., FIN) -----------
    data_chunks = rdt.chunk_file(filepath, mss)
    num_data = len(data_chunks)

    meta = rdt.build_start_metadata(
        filepath, filesize, mss, num_data, file_hash, "stopwait")

    packets = [rdt.Packet(rdt.TYPE_START, seq=0, payload=meta)]
    for i, chunk in enumerate(data_chunks, start=1):
        packets.append(rdt.Packet(rdt.TYPE_DATA, seq=i, payload=chunk))
    packets.append(rdt.Packet(rdt.TYPE_FIN, seq=num_data + 1))
    total = len(packets)

    # --- Socket + timer setup ----------------------------------------------
    sock = rdt.make_udp_socket()
    dest = (args.host, args.port)
    rtt = rdt.RttEstimator()
    if args.timeout is not None:
        rtt.use_fixed(args.timeout)

    log.info("Sending '%s' (%d bytes) to %s:%d via Stop-and-Wait "
             "[mss=%d, %d data packets]",
             os.path.basename(filepath), filesize, args.host, args.port,
             mss, num_data)

    retransmissions = 0
    packets_sent = 0
    start_time = rdt.now()

    # --- Main loop: one packet at a time -----------------------------------
    for seq in range(total):
        pkt = packets[seq]
        wire = pkt.encode()
        attempts = 0
        send_time = rdt.now()
        sock.sendto(wire, dest)
        packets_sent += 1
        log.debug("-> sent %s (attempt 1)", pkt)

        # Wait for the ACK that matches this packet's sequence number.
        while True:
            remaining = rtt.timeout() - (rdt.now() - send_time)
            if remaining <= 0:
                # Timeout: retransmit the same packet and restart the timer.
                attempts += 1
                if attempts > args.max_retries:
                    log.error("Gave up on seq=%d after %d retries", seq, attempts)
                    sock.close()
                    return 2
                retransmissions += 1
                packets_sent += 1
                send_time = rdt.now()
                sock.sendto(wire, dest)
                log.debug("   timeout, retransmit %s (attempt %d)", pkt, attempts + 1)
                continue

            ready = select.select([sock], [], [], remaining)[0]
            if not ready:
                continue  # loop back, the timeout branch above will fire

            raw, _ = sock.recvfrom(rdt.RECV_BUFFER)

            # Optional reverse-path loss simulation on the sender side.
            if rdt.should_drop(args.loss):
                log.debug("   (simulated) dropped an incoming ACK")
                continue

            ack = rdt.Packet.decode(raw)
            if ack is None or ack.ptype != rdt.TYPE_ACK:
                continue  # corrupted or not an ACK: ignore and keep waiting

            if ack.ack == seq:
                # Correct ACK. Sample RTT only if we never retransmitted (Karn).
                if attempts == 0:
                    rtt.update(rdt.now() - send_time)
                log.debug("<- ACK %d ok", ack.ack)
                break
            else:
                # A stale/duplicate ACK for an older packet: ignore it.
                log.debug("<- stale ACK %d (waiting for %d)", ack.ack, seq)
                continue

    elapsed = rdt.now() - start_time
    sock.close()

    throughput = filesize / elapsed if elapsed > 0 else 0.0
    report_results(args, log, "stopwait", filepath, filesize, mss, 1,
                   num_data, packets_sent, retransmissions, elapsed, throughput)

    if generated and os.path.exists(generated):
        os.remove(generated)
    return 0


def report_results(args, log, protocol, filepath, filesize, mss, window,
                   num_data, packets_sent, retransmissions, elapsed, throughput):
    """Print a human summary, and a JSON line too when --json is set."""
    log.info("Transfer complete in %.4fs", elapsed)
    log.info("  file size       : %d bytes", filesize)
    log.info("  data packets    : %d", num_data)
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
    parser = argparse.ArgumentParser(description="Stop-and-Wait reliable sender over UDP")
    rdt.add_common_net_args(parser, is_sender=True)
    rdt.add_sender_io_args(parser)
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())