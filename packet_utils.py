import struct


HEADER_FORMAT = '!IIB'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# User defined types [cite: 37]
TYPE_DATA = 0
TYPE_ACK = 1
TYPE_START = 2  # notify the server a file is to be transferred [cite: 45]
TYPE_EOF = 3    # notify the completion of file transfer [cite: 52]

def make_packet(seq_num, ack_num, pkt_type, payload=b''):
    """Encodes the header and attaches the payload."""
    header = struct.pack(HEADER_FORMAT, seq_num, ack_num, pkt_type)
    return header + payload

def parse_packet(packet):
    """Decodes the binary packet into its fields."""
    header = struct.unpack(HEADER_FORMAT, packet[:HEADER_SIZE])
    seq_num, ack_num, pkt_type = header[0], header[1], header[2]
    payload = packet[HEADER_SIZE:]
    return seq_num, ack_num, pkt_type, payload