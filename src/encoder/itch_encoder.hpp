#pragma once
#include "sprc/sprc.hpp"
#include <simdjson.h>
#include <string>
#include <string_view>
#include <atomic>
#include <array>
#include <vector>

using RingA = sprc::Ring<sprc::RawFrameData,   sprc::RING_SIZE_A>;
using RingB = sprc::Ring<sprc::EncodedPktData, sprc::RING_SIZE_B>;

class ItchEncoder {
    public:
        ItchEncoder(std::vector<RingA*> ring_list, RingB& ring_b);
        void run();

    private:
        std::vector<RingA*> ring_list_; // each pointer is one source (coinbase, binance...)
        RingB& ring_b_;

        void process_frame(const sprc::RawFrameData& frame);
        void flush_slot();

        // using ParseFn = std::function<void(std::string_view, sprc::EncodedPktData&)>;
        simdjson::ondemand::parser sj_parser_;  // reused across frames — never recreate

        using ParseFn = void (ItchEncoder::*)(std::string_view); //No `std::function`, no heap allocation, no type erasure — just a direct call through a stored pointer.
        std::array<ParseFn, 8> parsers_;

        std::atomic<uint64_t> seq_{0}; //global sequence counter, increments per message

        sprc::EncodedPktData pending_slot_{};
        uint8_t* write_cursor_; // points into pending_slot_.payload
        uint16_t msg_count_{0};
        uint64_t first_seq_{0};
        uint64_t last_flush_ns_{0};

        static constexpr std::size_t MOLD_HEADER_SIZE = 20;
        static constexpr std::size_t MAX_PAYLOAD = 1452; // 1472 - MOLD_HEADER_SIZE
        static constexpr uint64_t FLUSH_INTERVAL_NS = 50'000; // 50 µs latency cap

        inline uint64_t now_ns() noexcept {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL
                + static_cast<uint64_t>(ts.tv_nsec);
        }

        void parse_coinbase(std::string_view payload);
        void parse_binance (std::string_view payload);
};
