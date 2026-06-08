"""
UDP Multicast Subscriber with TCP gap-fill / retransmit verification.

Joins the multicast group, decodes incoming MoldUDP64 + ITCH packets, and
periodically simulates a "lost packet" by requesting a random already-received
sequence number over the TCP retransmit channel, then byte-compares the unicast
replay against the locally stored original.

Usage:
    python3 misc/udp_subscriber.py

Ports (must match publisher / main.cpp):
    9000  — UDP multicast  (live feed)
    19001 — TCP            (retransmit request channel)
    19002 — UDP unicast    (publisher sends replayed packets here)
"""

import random
import socket
import struct
import threading
import time
from collections import OrderedDict
from typing import Optional

# ── Configuration ──────────────────────────────────────────────────────────────

MCAST_GROUP        = "239.1.1.1"
MCAST_PORT         = 9000
TCP_PORT           = 19001    # publisher retransmit TCP server
UNICAST_PORT       = 19002    # we listen here for unicast gap-fills
PUBLISHER_HOST     = "127.0.0.1"

RETRANSMIT_INTERVAL  = 5.0    # seconds between gap-fill tests
RETRANSMIT_WARMUP    = 8.0    # wait this long before first request (let packets accumulate)
MAX_STORED_PKTS      = 2000   # rolling window of raw multicast packets

# ── Wire formats (big-endian — encoder applies hton* before writing) ──────────
#
# MoldHeader : session(10) + seq_num(8) + msg_count(2) = 20 bytes
# PriceTick  : msg_type(1) + symbol(8)  + price(8) + bid(8) + ask(8) + ts(8)  = 41 bytes
# AddOrder   : msg_type(1) + order_ref(8) + side(1) + qty(4) + symbol(8) + price(8) = 30 bytes
# OrderDelete: msg_type(1) + order_ref(8) + symbol(8) = 17 bytes
# RetransmitReq  : start_seq(8) + count(2) = 10 bytes
# RetransmitResp : status(1)               =  1 byte

MOLD_FMT = ">10sQH"
TICK_FMT = ">c8sQQQQ"
ADD_FMT  = ">cQcI8sQ"
DEL_FMT  = ">cQ8s"
REQ_FMT  = ">QH"
RESP_FMT = ">B"

MOLD_SIZE = struct.calcsize(MOLD_FMT)   # 20
TICK_SIZE = struct.calcsize(TICK_FMT)   # 41
ADD_SIZE  = struct.calcsize(ADD_FMT)    # 30
DEL_SIZE  = struct.calcsize(DEL_FMT)   # 17
REQ_SIZE  = struct.calcsize(REQ_FMT)   # 10
RESP_SIZE = struct.calcsize(RESP_FMT)  # 1

MSG_SIZES = {b'P': TICK_SIZE, b'A': ADD_SIZE, b'D': DEL_SIZE}

PRICE_SCALE = 1e8

# ── Shared packet store ────────────────────────────────────────────────────────
# keyed by MoldHeader.seq_num (first message seq of that packet)
_store_lock   = threading.Lock()
_seen_packets: "OrderedDict[int, bytes]" = OrderedDict()

# ── Terminal colours ───────────────────────────────────────────────────────────
C = {
    "green":   "\033[32m",
    "cyan":    "\033[36m",
    "red":     "\033[31m",
    "yellow":  "\033[33m",
    "magenta": "\033[35m",
    "reset":   "\033[0m",
    "dim":     "\033[90m",
    "bold":    "\033[1m",
}


# ── ITCH decode helpers ────────────────────────────────────────────────────────

def _px(scaled: int) -> str:
    return f"{scaled / PRICE_SCALE:.2f}"


def _sym(raw: bytes) -> str:
    return raw.decode("ascii", errors="replace").strip()


def _print_msg(msg_type: bytes, data: bytes, seq: int, tag: str) -> None:
    t = tag
    if msg_type == b'P':
        if len(data) < TICK_SIZE:
            return
        _, symbol, price, bid, ask, _ts = struct.unpack_from(TICK_FMT, data)
        print(f"{C['green']}{t}[TICK ] seq={seq:<6} {_sym(symbol):<8}  "
              f"price={_px(price):>12}  bid={_px(bid):>12}  ask={_px(ask):>12}{C['reset']}")

    elif msg_type == b'A':
        if len(data) < ADD_SIZE:
            return
        _, ref, side, qty, symbol, price = struct.unpack_from(ADD_FMT, data)
        print(f"{C['cyan']}{t}[ADD  ] seq={seq:<6} {_sym(symbol):<8}  "
              f"side={side.decode():<2}  qty={qty:<12}  price={_px(price):>12}{C['reset']}")

    elif msg_type == b'D':
        if len(data) < DEL_SIZE:
            return
        _, ref, symbol = struct.unpack_from(DEL_FMT, data)
        print(f"{C['red']}{t}[DEL  ] seq={seq:<6} {_sym(symbol):<8}  ref={ref}{C['reset']}")

    else:
        print(f"{C['dim']}{t}[????] seq={seq} type={msg_type!r}{C['reset']}")


def decode_packet(raw: bytes, tag: str = "") -> Optional[int]:
    """
    Parse and print all ITCH messages inside a MoldUDP64 packet.
    Returns the packet's first seq_num, or None if the header is malformed.
    `tag` is a short prefix printed before each decoded line (e.g. "[MCAST] ").
    """
    if len(raw) < MOLD_SIZE:
        return None

    _session, first_seq, msg_count = struct.unpack_from(MOLD_FMT, raw)
    seq    = first_seq
    offset = MOLD_SIZE

    for _ in range(msg_count):
        if offset >= len(raw):
            break
        msg_type = raw[offset : offset + 1]
        _print_msg(msg_type, raw[offset:], seq, tag)
        size = MSG_SIZES.get(msg_type)
        if size is None:
            print(f"{C['dim']}{tag}[????] seq={seq} unknown type={msg_type!r} — "
                  f"dropping rest of packet{C['reset']}")
            break
        offset += size
        seq    += 1

    return first_seq


# ── Thread 1: unicast retransmit listener (port 9002) ─────────────────────────

def unicast_recv_loop() -> None:
    """
    Receives replayed packets sent by the publisher's retransmit handler over
    UDP unicast.  For each packet, compares the raw bytes to the original that
    was received on the multicast feed.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(("", UNICAST_PORT))
    sock.settimeout(1.0)

    print(f"[retransmit] unicast listener ready on :{UNICAST_PORT}\n")

    while True:
        try:
            raw, addr = sock.recvfrom(2048)
        except socket.timeout:
            continue

        if len(raw) < MOLD_SIZE:
            continue

        _session, seq, _cnt = struct.unpack_from(MOLD_FMT, raw)

        print(f"\n{C['magenta']}{C['bold']}"
              f"╔══ RETRANSMIT RECEIVED  seq={seq}  {len(raw)} bytes  from {addr[0]} ══╗"
              f"{C['reset']}")
        decode_packet(raw, tag="  RETX  ")

        # ── Compare against the original multicast packet ──────────────────
        with _store_lock:
            original = _seen_packets.get(seq)

        if original is None:
            print(f"{C['yellow']}  └─ seq={seq}: not in local store "
                  f"(evicted from rolling window or never seen via multicast){C['reset']}\n")

        elif original == raw:
            print(f"{C['green']}{C['bold']}"
                  f"  └─ seq={seq}: ✓  MATCH — retransmit is byte-for-byte identical "
                  f"to the original multicast packet{C['reset']}\n")

        else:
            print(f"{C['red']}{C['bold']}"
                  f"  └─ seq={seq}: ✗  MISMATCH — payloads differ!{C['reset']}")
            _diff_report(original, raw)


def _diff_report(orig: bytes, retx: bytes) -> None:
    print(f"  orig ({len(orig):4d} bytes): {orig.hex()}")
    print(f"  retx ({len(retx):4d} bytes): {retx.hex()}")
    # highlight first differing byte
    for i, (a, b) in enumerate(zip(orig, retx)):
        if a != b:
            print(f"  first diff at byte {i}: orig=0x{a:02x}  retx=0x{b:02x}")
            break
    print()


# ── Thread 2: periodic retransmit requester ───────────────────────────────────

def retransmit_requester() -> None:
    """
    Every RETRANSMIT_INTERVAL seconds, picks a random seq_num from the local
    store (simulating a "lost" packet), connects to the publisher's TCP
    retransmit server, and requests that single packet back.

    The publisher will send the replay to our unicast socket (port 9002).
    """
    time.sleep(RETRANSMIT_WARMUP)

    while True:
        time.sleep(RETRANSMIT_INTERVAL)

        with _store_lock:
            if not _seen_packets:
                continue
            target_seq = random.choice(list(_seen_packets.keys()))

        print(f"\n{C['yellow']}{C['bold']}"
              f"╔══ SIMULATING LOST PACKET  seq={target_seq} ══╗{C['reset']}")
        print(f"{C['yellow']}  Connecting to {PUBLISHER_HOST}:{TCP_PORT} to request retransmit...{C['reset']}")

        try:
            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp.settimeout(3.0)
            tcp.connect((PUBLISHER_HOST, TCP_PORT))

            # Send RetransmitReq: start_seq=target_seq, count=1
            tcp.sendall(struct.pack(REQ_FMT, target_seq, 1))

            resp_raw = _recv_exact(tcp, RESP_SIZE)
            if resp_raw is None:
                print(f"{C['red']}  TCP response truncated{C['reset']}\n")
                tcp.close()
                continue

            (status,) = struct.unpack(RESP_FMT, resp_raw)

            if status == 0:
                print(f"{C['yellow']}  Publisher ACK — replay incoming on unicast :{UNICAST_PORT}{C['reset']}")
            else:
                print(f"{C['red']}  Publisher status={status} — "
                      f"seq={target_seq} has been overwritten in the circular log{C['reset']}\n")

            tcp.close()

        except (ConnectionRefusedError, socket.timeout, OSError) as exc:
            print(f"{C['red']}  TCP error: {exc}{C['reset']}\n")


def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    """Read exactly n bytes from a TCP socket, returns None on short read."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# ── Main: multicast receiver + store ──────────────────────────────────────────

def main() -> None:
    threading.Thread(target=unicast_recv_loop,    daemon=True, name="unicast-recv").start()
    threading.Thread(target=retransmit_requester, daemon=True, name="retx-req").start()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(("", MCAST_PORT))

    # Join on INADDR_ANY — OS picks the same interface as the sender
    mreq = struct.pack("4s4s",
                       socket.inet_aton(MCAST_GROUP),
                       socket.inet_aton("0.0.0.0"))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"Joined {MCAST_GROUP}:{MCAST_PORT} — waiting for packets...\n")

    pkts = 0
    try:
        while True:
            raw, _ = sock.recvfrom(2048)
            pkts += 1

            if pkts == 1:
                print(f"[first multicast packet — {len(raw)} bytes]\n")

            first_seq = decode_packet(raw)   # decode + print (no tag = clean output)

            if first_seq is not None:
                with _store_lock:
                    _seen_packets[first_seq] = raw
                    # evict oldest entries once window is full
                    while len(_seen_packets) > MAX_STORED_PKTS:
                        _seen_packets.popitem(last=False)

    except KeyboardInterrupt:
        print(f"\nReceived {pkts} multicast packets total.")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
