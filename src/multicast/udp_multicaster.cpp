#include "multicast/udp_multicaster.hpp"
#include "itch/mold_header.hpp"
#include "sprc/sprc.hpp"
#include <arpa/inet.h>
#include <cstring>
#include <iostream>

// from_be64 is Linux-only; use ntohl-based swap that works on macOS too
static inline uint64_t from_be64(uint64_t v) {
    return (static_cast<uint64_t>(ntohl(static_cast<uint32_t>(v))) << 32)
         | ntohl(static_cast<uint32_t>(v >> 32));
}
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

UDPMulticaster::UDPMulticaster(RingB&      ring_b,
                                std::string mcast_group,
                                uint16_t    mcast_port,
                                uint16_t    tcp_port,
                                uint16_t    unicast_port)
    : ring_b_(ring_b)
    , mcast_group_(std::move(mcast_group))
    , mcast_port_(mcast_port)
    , tcp_port_(tcp_port)
    , unicast_port_(unicast_port)
    , circ_log_(CIRC_LOG_SIZE)   // heap-allocated
{
    init_socket();
    // TCP listener runs on its own thread completely independent of the sender
    std::thread([this]{ tcp_listener(); }).detach();
}

void UDPMulticaster::init_socket() {
    sock_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_ < 0) {
        std::cerr << "[multicaster] ERROR: socket() failed\n";
        return;
    }

    int ttl = 1;
    setsockopt(sock_, IPPROTO_IP, IP_MULTICAST_TTL,  &ttl,  sizeof(ttl));
    int loop = 1;
    setsockopt(sock_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));

    mcast_addr_.sin_family      = AF_INET;
    mcast_addr_.sin_port        = htons(mcast_port_);
    mcast_addr_.sin_addr.s_addr = inet_addr(mcast_group_.c_str());

    std::cerr << "[multicaster] socket ready → "
              << mcast_group_ << ":" << mcast_port_
              << "  tcp=" << tcp_port_
              << "  unicast=" << unicast_port_ << "\n";
}

// ── Hot sender loop ───────────────────────────────────────────────────────────

uint64_t UDPMulticaster::seq_from_pkt(const sprc::EncodedPktData& pkt) {
    // MoldHeader layout (packed): session(10) + seq_num(8) + msg_count(2)
    uint64_t seq_be;
    std::memcpy(&seq_be, pkt.payload + 10, sizeof(seq_be));
    return from_be64(seq_be);
}

void UDPMulticaster::run() {
    uint64_t pkt_count = 0;
    std::cerr << "[multicaster] run loop started, polling Ring B...\n";

    for (;;) {
        ring_b_.consume([&](const sprc::EncodedPktData& pkt) {
            const uint64_t seq = seq_from_pkt(pkt);

            // store in circular log before sending so retransmit threads
            // never read a slot the sender hasn't committed yet
            circ_log_[seq & CIRC_LOG_MASK] = pkt;
            last_seq_.store(seq, std::memory_order_release);

            sendto(sock_,
                   pkt.payload, pkt.length, 0,
                   reinterpret_cast<const sockaddr*>(&mcast_addr_),
                   sizeof(mcast_addr_));

            ++pkt_count;
            if (pkt_count <= 10 || pkt_count % 100 == 0)
                std::cerr << "[multicaster] pkt #" << pkt_count
                          << "  seq=" << seq
                          << "  bytes=" << pkt.length << "\n";
        });
    }
}

// ── TCP listener — accepts subscriber gap-fill requests ───────────────────────

void UDPMulticaster::tcp_listener() {
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) {
        std::cerr << "[retransmit] ERROR: tcp socket() failed\n";
        return;
    }

    int yes = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in bind_addr{};
    bind_addr.sin_family      = AF_INET;
    bind_addr.sin_port        = htons(tcp_port_);
    bind_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(srv, reinterpret_cast<sockaddr*>(&bind_addr), sizeof(bind_addr)) < 0) {
        std::cerr << "[retransmit] ERROR: bind() failed\n";
        return;
    }

    listen(srv, 32);
    std::cerr << "[retransmit] TCP listener ready on port " << tcp_port_ << "\n";

    for (;;) {
        sockaddr_in client_addr{};
        socklen_t   len  = sizeof(client_addr);
        int         conn = accept(srv, reinterpret_cast<sockaddr*>(&client_addr), &len);

        if (conn < 0) continue;

        // hand off to a dedicated thread immediately — listener never blocks
        std::thread([this, conn, client_addr]() mutable {
            handle_retransmit(conn, client_addr);
            close(conn);
        }).detach();
    }
}

// ── Per-subscriber retransmit handler (runs on its own thread) ────────────────

void UDPMulticaster::handle_retransmit(int conn, sockaddr_in client_addr) {
    RetransmitReq req{};
    if (recv(conn, &req, sizeof(req), MSG_WAITALL) != sizeof(req)) return;

    const uint64_t start = from_be64(req.start_seq);
    const uint16_t count = ntohs(req.count);

    // don't replay slots the sender might still be writing
    const uint64_t safe_last = last_seq_.load(std::memory_order_acquire);
    const uint64_t safe_end  = std::min<uint64_t>(start + count,
                                                   safe_last > 128
                                                       ? safe_last - 128
                                                       : 0);
    // send ACK before replaying so subscriber knows data is coming
    RetransmitResp resp{.status = 0};
    send(conn, &resp, sizeof(resp), 0);

    // create a unicast UDP socket just for this replay
    int udp = socket(AF_INET, SOCK_DGRAM, 0);

    sockaddr_in dest = client_addr;
    dest.sin_port    = htons(unicast_port_);

    uint64_t replayed = 0;
    for (uint64_t seq = start; seq < safe_end; ++seq) {
        const auto& pkt = circ_log_[seq & CIRC_LOG_MASK];

        if (pkt.length == 0 || seq_from_pkt(pkt) != seq) {
            // slot has been overwritten — log too shallow for this request
            resp.status = 1;
            send(conn, &resp, sizeof(resp), 0);
            break;
        }

        sendto(udp, pkt.payload, pkt.length, 0,
               reinterpret_cast<const sockaddr*>(&dest), sizeof(dest));
        ++replayed;
    }

    close(udp);
    std::cerr << "[retransmit] replayed " << replayed
              << " pkts to " << inet_ntoa(client_addr.sin_addr) << "\n";
}
