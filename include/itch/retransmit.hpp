#pragma once
#include <cstdint>

// TCP control messages exchanged between subscriber and publisher.
// All integers big-endian on the wire.

struct __attribute__((packed)) RetransmitReq {
    uint64_t start_seq;  // first missing sequence number
    uint16_t count;      // number of packets to replay
};

struct __attribute__((packed)) RetransmitResp {
    uint8_t status;      // 0 = OK, 1 = some packets unavailable (log overwritten)
};
