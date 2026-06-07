#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace sprc {

    inline constexpr std::size_t RING_SIZE_A = 1024;
    inline constexpr std::size_t RING_SIZE_B  = 4096;
    // inline constexpr uint64_t    RING_MASK_A  = RING_SIZE_A - 1;
    // inline constexpr uint64_t    RING_MASK_B  = RING_SIZE_B - 1;
    inline constexpr uint64_t    CURSOR_FREE  = UINT64_MAX;

    struct RawFrameData {
        uint64_t arrival_ts;       // wall-clock ns at frame receipt
        uint16_t length;
        uint8_t  source_id;        // encoder uses this to pick the right JSON parser (0=coinbase, 1=binance, ...)
        uint8_t  _pad[5];
        uint8_t  payload[8192];    // 8 KB — Coinbase level2 snapshots can spike
    };

    struct EncodedPktData{
      uint16_t lenght;
      uint64_t seq_num;
      uint8_t payload[1472]; //Ethernet MTU 1500 minus 20 bytes IP header minus 8 bytes UDP header
    };

    template<typename T>
    struct Slot {
        std::atomic<uint64_t> seq;
        T data;
    };

    struct alignas(64) Cursor {
        std::atomic<uint64_t> value{0};
        uint8_t _pad[64 - sizeof(std::atomic<uint64_t>)];
    };

    template<typename SlotData, std::size_t ring_size>
    class Ring {
        private:
            std::array<Slot<SlotData>, ring_size> slots_{};
            alignas(64) Cursor head_{};
            alignas(64) Cursor tail_{}; //single consumer ring

        public:
            static constexpr uint64_t MASK = ring_size - 1;
            static_assert((ring_size & MASK) == 0, "Ring size must be a power of 2");

            static std::unique_ptr<Ring> create() {
                return std::make_unique<Ring>();
            }

            // v will not let user to use this without using the return bool value
            [[nodiscard]] __attribute__((always_inline)) bool publish(const SlotData& data) noexcept {

                const uint64_t head = head_.value.load(std::memory_order_relaxed);
                const uint64_t tail = tail_.value.load(std::memory_order_acquire);

                if (head - tail >= ring_size){
                    return false; // ring full - caller bumps dropped frames
                }

                auto& slot = slots_[head & MASK];
                slot.data = data;

                slot.seq.store(head + 1, std::memory_order_release);
                head_.value.store(head + 1, std::memory_order_release);

                return true;

            }

            template<typename F>
            __attribute__((always_inline)) bool consume(F&& f) noexcept {
                const uint64_t tail = tail_.value.load(std::memory_order_relaxed);
                const auto& slot = slots_[tail & MASK];
                const uint64_t expected = tail + 1;

                if (slot.seq.load(std::memory_order_acquire) != expected){
                    return false; // slot not yet published
                }

                f(slot.data);

                tail_.value.store(tail + 1, std::memory_order_release);
                return true;
            }
    };
}
