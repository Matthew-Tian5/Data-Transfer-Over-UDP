markdown_content = """# CP372: Computer Networks (Spring 2026)
## Assignment 2: Reliable Data Transfer over UDP

This project implements and evaluates two fundamental reliable data transfer (RDT) protocols—**Stop-and-Wait** and **Go-Back-N (GBN)**—built entirely on top of standard Python UDP sockets. Since UDP is inherently an unreliable transport protocol, this project layers custom transport mechanisms (sequence numbers, ACKs, timeouts, and retransmissions) to guarantee 100% reliable file delivery over a lossy simulated network channel.

---

### 1. System Requirements & Prerequisites

- **Python Runtime:** Python 3.x (Python 3.8+ recommended)
- **Standard Libraries Only:** The codebase strictly utilizes Python's built-in networking and concurrency toolsets. No external third-party frameworks (e.g., Twisted, Asyncio external modules, Scapy) are required or permitted:
  - `socket` — Network communication via UDP datagrams
  - `struct` — Strict binary serialization for packet headers
  - `time` — Interval calculations, precision timers, and benchmarks
  - `threading` — Asynchronous sliding window control loops for GBN
  - `random` — Stochastic packet loss simulation

---

### 2. Project Repository Structure

Ensure the following files are located within the same root directory:

| File Name | Description |
| :--- | :--- |
| **`packet_utils.py`** | Shared utilities containing the custom binary packet layout, structural serialization via `struct`, and packet types. |
| **`sender_stopwait.py`** | Core implementation of the Stop-and-Wait Sender protocol. |
| **`receiver_stopwait.py`**| Stop-and-Wait Receiver with integrated random packet loss simulation. |
| **`sender_gbn.py`** | Multi-threaded Go-Back-N (GBN) Sliding Window Sender. |
| **`receiver_gbn.py`** | Go-Back-N Receiver utilizing cumulative ACKs and random loss simulation. |
| **`README.md`** | This comprehensive documentation and operation guide. |

---

### 3. Custom Packet Specification & Format

To achieve low-overhead serialization and mimic real-world network frames, data is converted into a network byte-ordered (big-endian) binary sequence:

#### Header Structure
- **Sequence Number:** 4-byte Unsigned Integer (`I`) — Tracks packet positions.
- **ACK Number:** 4-byte Unsigned Integer (`I`) — Identifies acknowledged frames.
- **Packet Type:** 1-byte Unsigned Character (`B`) — Specifies operational codes.

**Total Header Size:** 9 Bytes