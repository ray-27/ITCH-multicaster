#pragma once
#include "encoder/itch_encoder.hpp"
#include "itch/retransmit.hpp"
#include <atomic>
#include <cstdint>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <vector>

class UDPMulticaster {
public:
    UDPMulticaster(RingB&       ring_b,
                   std::string  mcast_group,
                   uint16_t     mcast_port,
                   uint16_t     tcp_port      = 9001,
                   uint16_t     unicast_port  = 9002);
    void run();   // call on dedicated sender thread

private:
    void init_socket();
    void tcp_listener();
    void handle_retransmit(int conn, sockaddr_in client_addr);

    static uint64_t seq_from_pkt(const sprc::EncodedPktData& pkt);

    // multicast sender 
    RingB&      ring_b_;
    std::string mcast_group_;
    uint16_t    mcast_port_;
    int         sock_{-1};
    sockaddr_in mcast_addr_{};

    // tcp 
    uint16_t    tcp_port_;
    uint16_t    unicast_port_;

    // 64K-slot circular log — heap-allocated (~97 MB, far too large for stack)
    static constexpr std::size_t CIRC_LOG_SIZE = 65536;
    static constexpr uint64_t    CIRC_LOG_MASK = CIRC_LOG_SIZE - 1;
    std::vector<sprc::EncodedPktData> circ_log_;

    // highest sequence written by the sender retransmit threads won't
    // replay the last 128 slots to avoid reading a partially written entry
    std::atomic<uint64_t> last_seq_{0};
};
