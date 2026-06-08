#include "multicast/udp_multicaster.hpp"
#include "sprc/sprc.hpp"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <iostream>

UDPMulticaster::UDPMulticaster(RingB& ring_b, std::string mcast_group, uint16_t port)
    : ring_b_(ring_b)
    , mcast_group_(std::move(mcast_group))
    , port_(port)
{
    init_socket();
}

void UDPMulticaster::init_socket() {
    sock_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_ < 0) {
        std::cerr << "[multicaster] ERROR: socket() failed\n";
        return;
    }

    int ttl = 1;
    setsockopt(sock_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

    int loop = 1;
    setsockopt(sock_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));

    addr_.sin_family      = AF_INET;
    addr_.sin_port        = htons(port_);
    addr_.sin_addr.s_addr = inet_addr(mcast_group_.c_str());

    std::cerr << "[multicaster] socket ready → " << mcast_group_ << ":" << port_ << "\n";
}

void UDPMulticaster::run() {
    uint64_t pkt_count = 0;

    std::cerr << "[multicaster] run loop started, polling Ring B...\n";

    for(;;){
        ring_b_.consume([&](const sprc::EncodedPktData& pkt){
            ssize_t sent = sendto(sock_,
                pkt.payload,
                pkt.length,
                0,
                reinterpret_cast<const sockaddr*>(&addr_),
                sizeof(addr_));

            ++pkt_count;

            // log every packet for the first 10, then every 100th
            if (pkt_count <= 10 || pkt_count % 100 == 0) {
                std::cerr << "[multicaster] pkt #" << pkt_count
                          << "  bytes=" << pkt.length
                          << "  sent=" << sent
                          << "\n";
            }
        });
    }
}
