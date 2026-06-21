import socket
import threading
import time
from packet_utils import *

SERVER_ADDR = ('127.0.0.1', 8081)
TIMEOUT = 0.5
WINDOW_SIZE = 4 # [cite: 79, 86]
PAYLOAD_SIZE = 1024
FILE_TO_SEND = "test_file.txt"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(0.1)  # Short timeout for non-blocking recv

base = 0         # [cite: 77]
nextseqnum = 0   # [cite: 78]
packets = []     # List to hold all generated packets
timer_start = None
lock = threading.Lock()

def receive_acks():
    """Thread to process cumulative ACKs."""
    global base, timer_start
    total_packets = len(packets)
    
    while base < total_packets:
        try:
            ack_pkt, _ = sock.recvfrom(2048)
            _, ack, ptype, _ = parse_packet(ack_pkt)
            
            if ptype == TYPE_ACK:
                with lock:
                    # Cumulative ACK logic [cite: 83]
                    if ack >= base:
                        base = ack + 1
                        # Manage timer for oldest unacknowledged packet [cite: 82]
                        if base == nextseqnum:
                            timer_start = None
                        else:
                            timer_start = time.time()
        except socket.timeout:
            pass

def run():
    global base, nextseqnum, timer_start, packets

    # Load entire file into packet buffer
    packets.append(make_packet(0, 0, TYPE_START))
    with open(FILE_TO_SEND, "rb") as f:
        seq = 1
        while chunk := f.read(PAYLOAD_SIZE):
            packets.append(make_packet(seq, 0, TYPE_DATA, chunk))
            seq += 1
    packets.append(make_packet(seq, 0, TYPE_EOF))
    
    total_packets = len(packets)
    print(f"Loaded {total_packets} packets for GBN transmission.")

    ack_thread = threading.Thread(target=receive_acks)
    ack_thread.start()

    while base < total_packets:
        with lock:
            # Send multiple packets without waiting [cite: 81]
            while nextseqnum < base + WINDOW_SIZE and nextseqnum < total_packets:
                sock.sendto(packets[nextseqnum], SERVER_ADDR)
                if base == nextseqnum:
                    timer_start = time.time() # Start timer [cite: 82]
                nextseqnum += 1

            # Retransmit window after timeout [cite: 84]
            if timer_start and (time.time() - timer_start) > TIMEOUT:
                print(f"Timeout! Retransmitting window from base {base}...")
                timer_start = time.time()
                for i in range(base, nextseqnum):
                    sock.sendto(packets[i], SERVER_ADDR)

    ack_thread.join()
    print("GBN file transfer complete.")

if __name__ == "__main__":
    run()