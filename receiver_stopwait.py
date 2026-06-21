import socket
import random
from packet_utils import *

LISTEN_ADDR = ('127.0.0.1', 8080)
LOSS_RATE = 0.1  # Change to 0.0, 0.1, 0.2, 0.3 for testing [cite: 66, 67, 68, 69]

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(LISTEN_ADDR)

def run():
    print(f"Stop-and-Wait Receiver listening on {LISTEN_ADDR} with {LOSS_RATE*100}% loss rate...")
    expected_seq = 0
    f = None

    while True:
        packet, addr = sock.recvfrom(2048)
        
        # Simulated Packet Loss [cite: 61, 63, 64]
        if random.random() < LOSS_RATE:
            continue

        seq, ack, ptype, payload = parse_packet(packet)

        # Handle Start [cite: 57]
        if ptype == TYPE_START:
            if f: f.close()
            f = open("output_sw.txt", "wb")
            expected_seq = 1
            sock.sendto(make_packet(0, 0, TYPE_ACK), addr)

        # Handle Data [cite: 56, 57, 59]
        elif ptype == TYPE_DATA:
            if seq == expected_seq:
                if f: f.write(payload)
                expected_seq += 1
            
            # Send ACK for the last successfully received in-order packet [cite: 56]
            sock.sendto(make_packet(0, expected_seq - 1, TYPE_ACK), addr)

        # Handle EOF [cite: 58]
        elif ptype == TYPE_EOF:
            if f: f.close()
            sock.sendto(make_packet(0, seq, TYPE_ACK), addr)
            print("File received successfully.")
            break

if __name__ == "__main__":
    run()