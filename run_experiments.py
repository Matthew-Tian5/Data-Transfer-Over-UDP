#!/usr/bin/env python3
"""
run_experiments.py
==================

Performance evaluation harness (CP372 Assignment 2, Part D).

For every combination of (protocol, file size, loss rate) it runs a number of
trials. Each trial:

  1. generates / reuses one random test file of the requested size,
  2. starts the matching receiver as a subprocess and waits for its READY
     signal (delivered over an OS pipe, so there is no race where the sender
     starts before the receiver has bound its socket),
  3. runs the matching sender with --json and captures the metrics line,
  4. verifies the received file byte-for-byte against the original,
  5. tears the receiver down.

The per-trial numbers are then averaged over all trials and written out, and a
set of readable pivot tables (file size x loss rate) is printed for each metric.

Important modelling choices
---------------------------
  * Packet loss is simulated at the RECEIVER only (this matches the assignment:
    "simulate packet loss at the receiver"). The forward DATA packets are the
    ones dropped; ACKs are not dropped.
  * A FIXED retransmission timeout is used for every run (default 30 ms). This
    keeps the experiment reproducible and side-steps the fact that an adaptive
    timer would converge differently for each protocol. On loopback the real
    RTT is well under a millisecond, so a 30 ms timeout never fires unless a
    packet was actually lost.

Default file sizes are a practical subset (10KB .. 1MB) because the largest
sizes in the assignment (up to 100MB) take a very long time at 20-30% loss,
especially for Stop-and-Wait and Go-Back-N. Pass --include-large to add the big
sizes, or set your own with --sizes.

Usage examples
--------------
    python run_experiments.py
    python run_experiments.py --trials 5 --protocols stopwait gbn sr
    python run_experiments.py --sizes 10K 100K 1M --losses 0 0.1 0.2 0.3
    python run_experiments.py --include-large            # adds 5M..100M
    python run_experiments.py --out-dir results --window 16 --timeout 0.03
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import time

import rdt_common as rdt

SENDERS = {
    "stopwait": "sender_stopwait.py",
    "gbn": "sender_gbn.py",
    "sr": "sender_sr.py",
}
RECEIVERS = {
    "stopwait": "receiver_stopwait.py",
    "gbn": "receiver_gbn.py",
    "sr": "receiver_sr.py",
}
PRETTY = {"stopwait": "Stop-and-Wait", "gbn": "Go-Back-N", "sr": "Selective Repeat"}

DEFAULT_SIZES = ["10K", "50K", "100K", "500K", "1M"]
LARGE_SIZES = ["5M", "10M", "50M", "100M"]
DEFAULT_LOSSES = [0.0, 0.1, 0.2, 0.3]


def wait_for_ready(read_fd, timeout):
    """Block until the receiver writes READY to the pipe, or time out."""
    import select
    ready, _, _ = select.select([read_fd], [], [], timeout)
    if not ready:
        return False
    try:
        data = os.read(read_fd, 16)
        return b"READY" in data
    except OSError:
        return False


def estimate_timeout(size_bytes, loss, base_timeout):
    """A generous per-trial wall-clock cap so a stuck run cannot hang forever."""
    # Rough: more data and more loss => more time. Always at least 20s.
    secs = 20 + (size_bytes / 1_000_000.0) * 30 * (1 + 4 * loss)
    return max(20.0, secs)


def run_trial(proto, filepath, port, loss, window, mss, fixed_timeout,
              out_dir, python):
    """Run one sender/receiver pair. Returns (metrics_dict, integrity_ok)."""
    sender = SENDERS[proto]
    receiver = RECEIVERS[proto]
    rx_out = os.path.join(out_dir, f"rx_{proto}_{port}")
    os.makedirs(rx_out, exist_ok=True)

    read_fd, write_fd = os.pipe()
    rcv_cmd = [python, receiver, "--port", str(port), "--loss", str(loss),
               "--output-dir", rx_out, "--ready-fd", str(write_fd), "--quiet"]
    if proto == "sr":
        # Only the Selective Repeat receiver takes a window (for its buffer);
        # the Go-Back-N receiver needs no window argument.
        rcv_cmd += ["--window", str(window)]

    rproc = subprocess.Popen(rcv_cmd, pass_fds=(write_fd,),
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.close(write_fd)  # parent keeps only the read end

    metrics = None
    integrity_ok = False
    try:
        if not wait_for_ready(read_fd, timeout=10.0):
            raise RuntimeError("receiver did not signal READY")

        snd_cmd = [python, sender, "--port", str(port), "--file", filepath,
                   "--loss", "0.0", "--timeout", str(fixed_timeout),
                   "--mss", str(mss), "--json", "--quiet"]
        if proto in ("gbn", "sr"):
            snd_cmd += ["--window", str(window)]

        wall_cap = estimate_timeout(os.path.getsize(filepath), loss, fixed_timeout)
        proc = subprocess.run(snd_cmd, capture_output=True, text=True,
                              timeout=wall_cap)
        line = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
        metrics = json.loads(line) if line else None

        # Wait for the receiver to finish writing + verifying, then compare.
        try:
            rproc.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            rproc.terminate()
        received = os.path.join(rx_out, os.path.basename(filepath))
        integrity_ok = _files_equal(filepath, received)
    finally:
        try:
            os.close(read_fd)
        except OSError:
            pass
        if rproc.poll() is None:
            rproc.terminate()
            try:
                rproc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                rproc.kill()
        # Clean up this trial's received file to save disk on big runs.
        try:
            received = os.path.join(rx_out, os.path.basename(filepath))
            if os.path.exists(received):
                os.remove(received)
            os.rmdir(rx_out)
        except OSError:
            pass

    return metrics, integrity_ok


def _files_equal(a, b):
    """Compare two files in chunks without loading them fully into memory."""
    try:
        if os.path.getsize(a) != os.path.getsize(b):
            return False
    except OSError:
        return False
    with open(a, "rb") as fa, open(b, "rb") as fb:
        while True:
            ba = fa.read(65536)
            bb = fb.read(65536)
            if ba != bb:
                return False
            if not ba:
                return True


def main():
    parser = argparse.ArgumentParser(
        description="Run the Part D performance experiments and write CSV + tables.")
    parser.add_argument("--protocols", nargs="+",
                        default=["stopwait", "gbn", "sr"],
                        choices=["stopwait", "gbn", "sr"],
                        help="which protocols to test (default: all three)")
    parser.add_argument("--sizes", nargs="+", default=None,
                        help="file sizes, e.g. 10K 100K 1M (default: 10K..1M)")
    parser.add_argument("--losses", nargs="+", type=float, default=DEFAULT_LOSSES,
                        help="loss rates to test (default: 0 0.1 0.2 0.3)")
    parser.add_argument("--include-large", action="store_true",
                        help="also test 5M, 10M, 50M, 100M (slow at high loss)")
    parser.add_argument("--trials", type=int, default=5,
                        help="trials per (protocol, size, loss) (default: 5)")
    parser.add_argument("--window", type=int, default=rdt.DEFAULT_WINDOW,
                        help="window size for GBN and SR (default: %(default)s)")
    parser.add_argument("--mss", type=int, default=rdt.DEFAULT_MSS,
                        help="max segment size in bytes (default: %(default)s)")
    parser.add_argument("--timeout", type=float, default=0.03,
                        help="fixed retransmission timeout in seconds "
                             "(default: %(default)s)")
    parser.add_argument("--base-port", type=int, default=5700,
                        help="first UDP port; each trial uses the next one")
    parser.add_argument("--out-dir", default="results",
                        help="directory for CSV output (default: results)")
    args = parser.parse_args()

    sizes = args.sizes if args.sizes else list(DEFAULT_SIZES)
    if args.include_large and not args.sizes:
        sizes = list(DEFAULT_SIZES) + list(LARGE_SIZES)
    size_bytes = {s: rdt.parse_size(s) for s in sizes}

    os.makedirs(args.out_dir, exist_ok=True)
    python = sys.executable

    # Generate one real random file per size, reused across all trials.
    tmp_dir = tempfile.mkdtemp(prefix="cp372_bench_")
    files = {}
    for s in sizes:
        path = os.path.join(tmp_dir, f"test_{s}.bin")
        rdt.make_random_file(path, size_bytes[s])
        files[s] = path
    print(f"Generated {len(files)} test file(s) in {tmp_dir}")

    per_trial_rows = []
    summary_rows = []
    port = args.base_port

    total_runs = len(args.protocols) * len(sizes) * len(args.losses) * args.trials
    done = 0
    t0 = time.time()

    for proto in args.protocols:
        for s in sizes:
            for loss in args.losses:
                times, thrus, retxs, sents = [], [], [], []
                all_ok = True
                for trial in range(1, args.trials + 1):
                    port += 1
                    try:
                        metrics, ok = run_trial(
                            proto, files[s], port, loss, args.window, args.mss,
                            args.timeout, args.out_dir, python)
                    except Exception as exc:  # one bad trial must not stop the run
                        metrics, ok = None, False
                        print(f"  [{done + 1}/{total_runs}] {proto} {s} "
                              f"loss={loss} trial {trial}: ERROR ({exc})")
                    done += 1
                    if metrics is None:
                        all_ok = False
                        print(f"  [{done}/{total_runs}] {proto} {s} loss={loss} "
                              f"trial {trial}: FAILED (no metrics)")
                        continue
                    if not ok:
                        all_ok = False
                    times.append(metrics["transfer_time"])
                    thrus.append(metrics["throughput"])
                    retxs.append(metrics["retransmissions"])
                    sents.append(metrics["packets_sent"])
                    per_trial_rows.append({
                        "protocol": proto,
                        "file_label": s,
                        "file_size_bytes": size_bytes[s],
                        "loss_rate": loss,
                        "trial": trial,
                        "transfer_time_s": metrics["transfer_time"],
                        "throughput_Bps": metrics["throughput"],
                        "retransmissions": metrics["retransmissions"],
                        "packets_sent": metrics["packets_sent"],
                        "data_packets": metrics["data_packets"],
                        "integrity_ok": ok,
                    })
                    print(f"  [{done}/{total_runs}] {proto} {s} loss={loss} "
                          f"trial {trial}: {'ok' if ok else 'INTEGRITY FAIL'} "
                          f"time={metrics['transfer_time']:.4f}s "
                          f"retx={metrics['retransmissions']}")

                n = len(times)
                summary_rows.append({
                    "protocol": proto,
                    "file_label": s,
                    "file_size_bytes": size_bytes[s],
                    "loss_rate": loss,
                    "trials": n,
                    "avg_transfer_time_s": round(sum(times) / n, 6) if n else "",
                    "avg_throughput_Bps": round(sum(thrus) / n, 3) if n else "",
                    "avg_retransmissions": round(sum(retxs) / n, 2) if n else "",
                    "avg_packets_sent": round(sum(sents) / n, 2) if n else "",
                    "all_integrity_ok": all_ok,
                })

    # --- Write the CSV files ----------------------------------------------
    per_trial_path = os.path.join(args.out_dir, "results_per_trial.csv")
    summary_path = os.path.join(args.out_dir, "results_summary.csv")
    _write_csv(per_trial_path, per_trial_rows)
    _write_csv(summary_path, summary_rows)

    elapsed = time.time() - t0
    print(f"\nFinished {done} runs in {elapsed:.1f}s")
    print(f"Wrote {per_trial_path}")
    print(f"Wrote {summary_path}")

    # --- Print readable pivot tables --------------------------------------
    _print_tables(summary_rows, args.protocols, sizes, args.losses)


def _write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _print_tables(summary_rows, protocols, sizes, losses):
    """Print one pivot table per (protocol, metric): rows=size, cols=loss."""
    index = {(r["protocol"], r["file_label"], r["loss_rate"]): r for r in summary_rows}
    metrics = [
        ("avg_transfer_time_s", "Average transfer time (seconds)"),
        ("avg_throughput_Bps", "Average throughput (bytes/sec)"),
        ("avg_retransmissions", "Average retransmissions (count)"),
    ]
    for proto in protocols:
        for key, title in metrics:
            print(f"\n=== {PRETTY[proto]} - {title} ===")
            header = "size".ljust(8) + "".join(f"{('loss '+str(int(l*100))+'%'):>16}"
                                                for l in losses)
            print(header)
            for s in sizes:
                row = s.ljust(8)
                for l in losses:
                    cell = index.get((proto, s, l), {}).get(key, "")
                    row += f"{str(cell):>16}"
                print(row)


if __name__ == "__main__":
    raise SystemExit(main())