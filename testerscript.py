import socket
import segment
from segment import RDTSegment

RECV_ADDR = ("127.0.0.1", 51003)  # replace with your receiver's actual listen port

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(1.0)
sock.bind(("127.0.0.1", 0))  # ephemeral port so we can receive ACKs back

def send_and_check(seq_num, payload, label):
    seg = RDTSegment(
        seqNum=segment.mask(seq_num),
        ackNum=0,
        flags=0,
        rcvWin=64,
        payload=payload,
    )
    sock.sendto(seg.pack(), RECV_ADDR)
    try:
        data, _ = sock.recvfrom(4096)
        ack = RDTSegment.unpack(data)
        print(f"{label}: got ACK ackNum={ack.ackNum} isAck={ack.isAck}")
    except socket.timeout:
        print(f"{label}: timed out waiting for ACK")

# 1. In-order segment 0
send_and_check(0, b"hello ", "in-order seq=0")

# 2. Duplicate segment 0 -> should re-ACK 0 (when expected == 1)
send_and_check(0, b"hello ", "dup seq=0")

# 3. Out-of-order segment 2 -> GBN with expected=1 should re-ACK 0, not advance
send_and_check(2, b"world", "out-of-order seq=2")

# 4. In-order segment 1 -> should advance expected to 2 and ACK 1
send_and_check(1, b"there", "in-order seq=1")

sock.close()