import socket
import time
from packet_utils import *

SERVER_ADDR = ('127.0.0.1', 8080)
TIMEOUT = 0.5
PAYLOAD_SIZE = 1024
FILE_TO_SEND = "test_file.txt" # Make sure this file exists in your directory [cite: 44]

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(TIMEOUT) # Start a timer after transmission [cite: 48]

def send_and_wait(packet, expected_ack):
    """Sends a packet and waits for the specific ACK, retransmitting on timeout."""
    while True:
        sock.sendto(packet, SERVER_ADDR)
        try:
            ack_pkt, _ = sock.recvfrom(2048)
            seq, ack, ptype, _ = parse_packet(ack_pkt)
            if ptype == TYPE_ACK and ack == expected_ack:
                return
        except socket.timeout:
            # Retransmit upon timeout [cite: 50]
            continue 

def run():
    # 1. Notify server of transfer [cite: 45]
    print("Sending START packet...")
    send_and_wait(make_packet(0, 0, TYPE_START), 0)

    # 2. Divide file and send packets [cite: 46, 47]
    seq_num = 1
    with open(FILE_TO_SEND, "rb") as f:
        while True:
            chunk = f.read(PAYLOAD_SIZE)
            if not chunk:
                break
            packet = make_packet(seq_num, 0, TYPE_DATA, chunk)
            send_and_wait(packet, seq_num)
            seq_num += 1

    # 3. Notify completion [cite: 52]
    print("Sending EOF packet...")
    send_and_wait(make_packet(seq_num, 0, TYPE_EOF), seq_num)
    print("File transfer complete.")

if __name__ == "__main__":
    run()