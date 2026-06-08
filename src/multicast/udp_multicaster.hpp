#pragma once
#include "encoder/itch_encoder.hpp"
#include <cstdint>
#include <netinet/in.h>
#include <string>

class UDPMulticaster {
    public:
        UDPMulticaster(RingB& ring_b,
                       std::string mcast_group,
                       uint16_t port);
        void run();

    private:
        void init_socket();

        RingB&      ring_b_;
        std::string mcast_group_;
        uint16_t    port_;
        int         sock_{-1};
        sockaddr_in addr_{};
};
