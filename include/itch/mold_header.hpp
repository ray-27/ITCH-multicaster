#pragma once
#include <cstdint>


struct __attribute__((packed)) MoldHeader {
    char     session[10];
    uint64_t seq_num;      // first message sequence in this packet — big-endian on wire (hton64 applied by encoder)
    uint16_t msg_count;
}; // sizeof = 20, no padding
