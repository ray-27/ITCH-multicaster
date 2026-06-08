#pragma once
#include <cstdint>


struct MoldHeader {
    char     session[10];
    uint64_t seq_num;      // first message sequence in this packet
    uint16_t msg_count;
};
