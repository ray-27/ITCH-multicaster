# itch-multicast

A C++ market data pipeline that ingests live price feeds from Coinbase Advanced Trade via WebSocket, encodes them into ITCH 5.0 binary format, and distributes them over UDP multicast. Subscribers receive a low-latency binary feed and can request gap-fill retransmits over TCP.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        publisher (single process)               │
│                                                                 │
│  Thread 1: WsIngestor                                           │
│    TLS WebSocket → Coinbase Advanced Trade                      │
│    raw JSON frames → Ring A (SPSC, lock-free)                   │
│                                                                 │
│  Thread 2: ItchEncoder                                          │
│    Ring A → simdjson parse → ITCH structs → MoldUDP64 framing   │
│    batched wire packets → Ring B (SPSC, lock-free)              │
│                                                                 │
│  Thread 3: UDPMulticaster                                       │
│    Ring B → circ_log (64K slots) → sendto() multicast          │
│                                                                 │
│  Thread 4: TCP Listener (detached)                              │
│    accepts retransmit requests → unicast UDP replay             │
└─────────────────────────────────────────────────────────────────┘
                          │ UDP multicast 239.1.1.1:9000
              ┌───────────┴───────────┐
         Subscriber A           Subscriber B
```

---

## Ports

| Port  | Protocol | Direction      | Purpose                                      |
|-------|----------|----------------|----------------------------------------------|
| 9000  | UDP      | publisher → *  | Multicast feed (join group 239.1.1.1)        |
| 19001 | TCP      | subscriber → publisher | Gap-fill retransmit requests        |
| 19002 | UDP      | publisher → subscriber | Unicast retransmit replay           |

---

## Wire Format

Every UDP packet on port 9000 is a **MoldUDP64** frame containing one or more **ITCH 5.0** messages.

### MoldUDP64 Header (20 bytes, big-endian)

| Field       | Type       | Size | Description                        |
|-------------|------------|------|------------------------------------|
| session     | char[10]   | 10   | Session identifier                 |
| seq_num     | uint64     | 8    | Sequence of first message in packet|
| msg_count   | uint16     | 2    | Number of ITCH messages following  |

### ITCH Message Types

#### PriceTick (`msg_type = 'P'`, 41 bytes)

| Field     | Type     | Size | Description              |
|-----------|----------|------|--------------------------|
| msg_type  | char     | 1    | `'P'`                    |
| symbol    | char[8]  | 8    | Space-padded, e.g. `BTC-USD` |
| price     | uint64   | 8    | Scaled ×10⁸              |
| best_bid  | uint64   | 8    | Scaled ×10⁸              |
| best_ask  | uint64   | 8    | Scaled ×10⁸              |
| timestamp | uint64   | 8    | Nanoseconds since epoch  |

#### AddOrder (`msg_type = 'A'`, 30 bytes)

| Field     | Type     | Size | Description              |
|-----------|----------|------|--------------------------|
| msg_type  | char     | 1    | `'A'`                    |
| order_ref | uint64   | 8    | Unique order reference   |
| side      | char     | 1    | `'B'` = bid, `'S'` = ask |
| quantity  | uint32   | 4    | Whole units              |
| symbol    | char[8]  | 8    | Space-padded             |
| price     | uint64   | 8    | Scaled ×10⁸              |

#### OrderDelete (`msg_type = 'D'`, 17 bytes)

| Field     | Type     | Size | Description              |
|-----------|----------|------|--------------------------|
| msg_type  | char     | 1    | `'D'`                    |
| order_ref | uint64   | 8    | Matches an AddOrder ref  |
| symbol    | char[8]  | 8    | Space-padded             |

All integers are **big-endian** on the wire. Prices are fixed-point integers scaled by 10⁸ — divide by `1e8` to get the decimal value (e.g. `6743215000000 / 1e8 = 67432.15`).

---

## Gap-Fill Protocol

If a subscriber detects a sequence gap it sends a retransmit request over TCP to port `19001`:

**Request (10 bytes, big-endian):**
```
uint64  start_seq   — first missing sequence number
uint16  count       — number of packets to replay
```

**Response (1 byte):**
```
uint8   status      — 0 = OK, 1 = packets unavailable (log overwritten)
```

Replayed packets arrive as unicast UDP on port `19002`. They are identical wire packets from the circular log — same MoldUDP64 framing, same sequence numbers.

The circular log holds the last **65,536 packets** (~97 MB). At typical rates this covers several minutes of history.

---

## Building

**Requirements:** macOS or Linux, CMake ≥ 3.20, C++20 compiler, Boost, OpenSSL.

```bash
# macOS (Homebrew)
brew install boost openssl@3

# configure — fetches simdjson automatically via FetchContent
cmake -B build -S .

# build
cmake --build build
```

---

## Running

```bash
./build/publisher
```

Expected startup output:
```
[multicaster] socket ready → 239.1.1.1:9000  tcp=19001  unicast=19002
[multicaster] run loop started, polling Ring B...
[retransmit] TCP listener ready on port 19001
[coinbase] connected
[multicaster] pkt #1  seq=0  bytes=61
```

---

## Subscribing

### Python subscriber (included)

Joins the multicast group and decodes packets to terminal:

```bash
python3 misc/udp_subscriber.py
```

Output:
```
[TICK ] seq=0      BTC-USD   price=    67432.15  bid=    67431.98  ask=    67432.98
[ADD  ] seq=2      ETH-USD   side=B  qty=1          price=     3201.44
```

### Custom subscriber (any language)

1. Create a UDP socket and join multicast group `239.1.1.1` on port `9000`
2. For each received packet, parse the 20-byte MoldHeader to get `seq_num` and `msg_count`
3. Walk through `msg_count` ITCH messages starting at byte offset 20
4. If `seq_num` is not `expected_seq`, connect TCP to port `19001` and request the missing range

---

## Project Structure

```
.
├── CMakeLists.txt
├── include/
│   ├── itch/
│   │   ├── itch_types.hpp      # AddOrder, OrderDelete, PriceTick structs
│   │   ├── mold_header.hpp     # MoldUDP64 framing header
│   │   └── retransmit.hpp      # TCP gap-fill request/response structs
│   └── sprc/
│       └── sprc.hpp            # SPSC ring template
├── src/
│   ├── main.cpp
│   ├── ingestor/
│   │   ├── ws_ingestor.hpp
│   │   └── ws_ingestor.cpp     # TLS WebSocket → Ring A
│   ├── encoder/
│   │   ├── itch_encoder.hpp
│   │   └── itch_encoder.cpp    # Ring A → ITCH encoder → Ring B
│   └── multicast/
│       ├── udp_multicaster.hpp
│       └── udp_multicaster.cpp # Ring B → UDP multicast + TCP gap-fill
└── misc/
    ├── udp_subscriber.py       # Python multicast subscriber
```
