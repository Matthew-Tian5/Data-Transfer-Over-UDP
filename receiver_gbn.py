import socket
import random
from packet_utils import *

LISTEN_ADDR = ('127.0.0.1', 8081)
LOSS_RATE = 0.1 # Adjust for testing [cite: 104-108]

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(LISTEN_ADDR)

def run():
    print(f"GBN Receiver listening on {LISTEN_ADDR} with {LOSS_RATE*100}% loss rate...")
    expected_seq = 0 # Expected packet tracker [cite: 98]
    f = None

    while True:
        packet, addr = sock.recvfrom(2048)
        
        if random.random() < LOSS_RATE:
            continue

        seq, ack, ptype, payload = parse_packet(packet)

        # Accept only the expected packet [cite: 94]
        if seq == expected_seq:
            if ptype == TYPE_START:
                if f: f.close()
                f = open("output_gbn.txt", "wb")
            elif ptype == TYPE_DATA:
                if f: f.write(payload)
            elif ptype == TYPE_EOF:
                if f: f.close()
                sock.sendto(make_packet(0, expected_seq, TYPE_ACK), addr)
                print("File received successfully.")
                break

            # Send cumulative ACK and advance expected [cite: 96]
            sock.sendto(make_packet(0, expected_seq, TYPE_ACK), addr)
            expected_seq += 1

        else:
            # Discard out-of-order packets and send ACK for last valid packet [cite: 95, 100, 101]
            sock.sendto(make_packet(0, expected_seq - 1, TYPE_ACK), addr)

if __name__ == "__main__":
    run()