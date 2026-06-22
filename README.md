CP372 Assignment 2 - Reliable Data Transfer over UDP
=====================================================

This project implements and compares reliable data transfer protocols built on
top of plain UDP sockets in Python. Everything uses only the Python standard
library, no third party networking packages.

Protocols included:
  - Stop-and-Wait        (Part A and B)
  - Go-Back-N            (Part C)
  - Selective Repeat     (Bonus Option A, single timer)

Plus packet loss simulation (Part B), packet corruption with checksums
(Bonus Option B), and a benchmarking script for the performance evaluation
(Part D).


REQUIREMENTS
------------
  - Python 3.8 or newer (developed and tested on Python 3.12).
  - No third party packages. Standard library only.
  - Works on Linux, macOS, and Windows. The examples below use the "python"
    command, so on some systems you may need "python3" instead.


FILES
-----
  rdt_common.py          Shared code used by every program: the packet format,
                         the 16-bit Internet checksum, socket setup, the
                         adaptive timeout estimator, the loss and corruption
                         helpers, and the command line argument helpers.

  sender_stopwait.py     Stop-and-Wait sender.
  receiver_stopwait.py   Stop-and-Wait receiver.

  sender_gbn.py          Go-Back-N sender.
  receiver_gbn.py        Go-Back-N receiver.

  sender_sr.py           Selective Repeat sender (bonus).
  receiver_sr.py         Selective Repeat receiver (bonus).

  run_experiments.py     Runs the Part D experiments and writes CSV results
                         plus readable tables.

  Readme.txt             This file.
  Bonus.txt              Describes the two bonus features.

IMPORTANT: keep all of the .py files in the same folder. Every sender and
receiver imports rdt_common, so they will not run if that file is missing or in
a different directory.


HOW TO RUN
----------
Each transfer needs two programs running at the same time: start the RECEIVER
first, then start the SENDER in a second terminal. The receiver binds a UDP
port and waits, the sender connects to that port and pushes the file across.

The receiver writes the file it receives into a folder called received_files by
default. You can change that with --output-dir.

Stop-and-Wait example:

  Terminal 1 (receiver):
    python receiver_stopwait.py --port 5005

  Terminal 2 (sender):
    python sender_stopwait.py --file myfile.txt --port 5005

Go-Back-N example (note the sliding window size):

  Terminal 1:
    python receiver_gbn.py --port 5005

  Terminal 2:
    python sender_gbn.py --file myfile.txt --port 5005 --window 16

Selective Repeat example:

  Terminal 1:
    python receiver_sr.py --port 5005 --window 16

  Terminal 2:
    python sender_sr.py --file myfile.txt --port 5005 --window 16

If you do not have a file handy, the sender can make a random one for you with
--size instead of --file:

    python sender_gbn.py --size 1M --port 5005 --window 16

When the transfer finishes the receiver prints whether the received file
matches the original by size and by SHA-256 hash, so you get an immediate
INTEGRITY OK or INTEGRITY MISMATCH message.


SIMULATING PACKET LOSS (Part B)
-------------------------------
Loss is simulated at the RECEIVER. Pass --loss with a value between 0 and 1
(so 0.1 means a 10 percent chance each incoming data packet is thrown away).
A dropped packet is never acknowledged, so the sender's timer eventually fires
and it retransmits. The file still arrives correctly, it just takes more
packets and more time.

  python receiver_gbn.py --port 5005 --loss 0.2
  python sender_gbn.py --file myfile.txt --port 5005 --window 16

Try 0.0, 0.1, 0.2, and 0.3 to see how each protocol holds up.


SIMULATING CORRUPTION (Bonus Option B)
--------------------------------------
The receiver can also flip a random bit in incoming datagrams to mimic
corruption. Pass --corruption with a value between 0 and 1. The 16-bit
checksum in every packet catches the damage and the packet is discarded just
like a lost one, so the file still comes through intact.

  python receiver_sr.py --port 5005 --corruption 0.05 --window 16
  python sender_sr.py --file myfile.txt --port 5005 --window 16


RUNNING THE PERFORMANCE EXPERIMENTS (Part D)
--------------------------------------------
run_experiments.py automates the whole evaluation. It starts a receiver, runs
a sender, captures the metrics, repeats for several trials, and averages the
results. You do not need two terminals for this, it launches both sides itself.

Run the defaults (all three protocols, file sizes 10KB up to 1MB, loss rates
0, 10, 20, 30 percent, 5 trials each):

  python run_experiments.py

It writes two CSV files into a results folder:
  results/results_per_trial.csv   every individual run
  results/results_summary.csv     the averaged numbers per setting

It also prints readable tables to the screen, one per protocol per metric, laid
out as file size down the side and loss rate across the top. Those tables map
straight onto what Part D asks for (average transfer time, average throughput,
and average retransmissions).

Some useful options:
  --protocols stopwait gbn sr   pick which protocols to run
  --sizes 10K 100K 1M           pick your own file sizes
  --losses 0 0.1 0.2 0.3        pick your own loss rates
  --trials 5                    how many runs to average per setting
  --window 16                   window size for Go-Back-N and Selective Repeat
  --timeout 0.03                fixed retransmission timeout in seconds
  --include-large               also test 5M, 10M, 50M, 100M
  --out-dir results             where to put the CSV files

A note on the big file sizes: the assignment lists sizes up to 100MB. Those run
fine at 0 percent loss, but at 20 or 30 percent loss they get slow, especially
for Stop-and-Wait and Go-Back-N, because so much gets retransmitted. That is why
the script defaults to the 10KB to 1MB range. Use --include-large (or pass your
own --sizes) when you want the full set, and expect the high loss runs on large
files to take a while.


COMMAND LINE OPTIONS (senders and receivers)
--------------------------------------------
Shared by all programs:
  --host HOST          address to use (default 127.0.0.1)
  --port PORT          UDP port (default 5005)
  --verbose            print detailed per packet logs
  --quiet              only print warnings and errors

Senders:
  --file FILE          the file to send
  --size SIZE          instead of --file, generate a random file of this size
                       (accepts forms like 500, 10K, 1M, 100M)
  --mss BYTES          max payload bytes per packet (default 1024)
  --timeout SECONDS    use a fixed retransmission timeout instead of the
                       adaptive one
  --loss RATE          chance of dropping an incoming ACK (reverse path loss,
                       0 by default, normally left off)
  --json               print one machine readable line of metrics, used by the
                       experiment script
  --max-retries N      give up on a packet after this many retransmits
                       (default 10000, basically never)
  --window SIZE        sliding window size, Go-Back-N and Selective Repeat only
                       (default 16)

Receivers:
  --loss RATE          chance of dropping an incoming data packet (Part B)
  --corruption RATE    chance of flipping a random bit in a datagram (bonus)
  --output-dir DIR     where to write the received file (default received_files)
  --linger SECONDS     how long to keep answering duplicate packets after the
                       transfer ends (default 1.0)
  --idle-timeout SECS  give up if no packets arrive for this long (default 30)
  --ready-fd FD        write READY to this file descriptor once bound, used by
                       the experiment script to know the receiver is up
  --window SIZE        receive window, Selective Repeat receiver only
                       (should be at least as large as the sender's window)


ASSUMPTIONS AND DESIGN NOTES
----------------------------
  - Shared module. All the common pieces live in rdt_common.py so the four main
    programs stay short and there is only one copy of the packet format and
    checksum. The tradeoff is that the files must travel together.

  - One sequence space per transfer. A transfer is one ordered list of packets:
    a START packet (sequence 0) that carries the file name, size, and SHA-256 as
    JSON, then the DATA packets (1 to N), then a FIN packet (N+1). Because the
    receiver only ever delivers packets in order, the natural result is "create
    the file, write the data, close the file" without any extra signalling.

  - Absolute sequence numbers with no wraparound. Sequence numbers are 32 bit
    and just count up. A real protocol would wrap them, but a 32 bit counter
    covers four billion packets, which is far more than any file size here, so
    skipping wraparound keeps the window logic simple and avoids a whole class
    of off by one bugs.

  - Packet format. Each packet starts with a 13 byte header (type, sequence
    number, ack number, checksum, payload length) followed by the payload. The
    checksum is the standard 16 bit Internet checksum computed over the whole
    packet.

  - ACK meaning. Stop-and-Wait and Go-Back-N use cumulative ACKs, where an ACK
    for sequence number a means everything up to and including a arrived.
    Selective Repeat uses individual ACKs, where an ACK names the one packet
    that arrived. The senders react accordingly.

  - Timeout. By default the senders estimate the timeout from observed round
    trip times (the same smoothing TCP uses, with Karn's rule of not sampling
    retransmitted packets). Pass --timeout to force a fixed value. The
    experiment script always uses a fixed timeout so its numbers are
    reproducible.

  - Loss is at the receiver. For the experiments, loss is applied to the forward
    direction only, at the receiver, which is what the assignment describes. The
    senders do have a --loss option for dropping ACKs, but it is off by default.

  - Integrity check. The receiver compares the received file against the size
    and SHA-256 the sender put in the START packet, and reports the result. This
    is just a confidence check, it is not part of the protocol itself.

  - The received file keeps its original name and lands in the output directory.
    Running two transfers of the same file into the same directory will
    overwrite the earlier copy.