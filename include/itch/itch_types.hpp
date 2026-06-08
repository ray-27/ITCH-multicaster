#pragma once
#include <cstdint>

struct __attribute__((packed)) AddOrder {
    char     msg_type = 'A';
    uint64_t order_ref;
    char     side;        // 'B' or 'S'
    uint32_t quantity;
    char     symbol[8];
    uint64_t price;       // scaled by 1e8
}; // sizeof = 30

struct __attribute__((packed)) OrderDelete {
    char     msg_type = 'D';
    uint64_t order_ref;
    char     symbol[8];
}; // sizeof = 17

struct __attribute__((packed)) PriceTick {
    char     msg_type = 'P';
    char     symbol[8];
    uint64_t price;       // scaled by 1e8
    uint64_t best_bid;    // scaled by 1e8
    uint64_t best_ask;    // scaled by 1e8
    uint64_t timestamp;   // ns since epoch
}; // sizeof = 41
