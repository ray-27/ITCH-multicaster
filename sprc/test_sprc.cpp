#include "sprc.hpp"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <thread>

// ── helpers ──────────────────────────────────────────────────────────────────

static int g_passed = 0;
static int g_failed = 0;

#define CHECK(cond, name)                                          \
    do {                                                           \
        if (cond) { std::printf("  PASS  %s\n", name); ++g_passed; } \
        else      { std::printf("  FAIL  %s  (line %d)\n", name, __LINE__); ++g_failed; } \
    } while (0)

static void section(const char* title) {
    std::printf("\n── %s\n", title);
}

// ── test helpers ─────────────────────────────────────────────────────────────

using SmallRing = sprc::Ring<sprc::EncodedPktData, 8>;   // tiny ring for boundary tests
using BigRingA  = sprc::Ring<sprc::RawFrameData,   sprc::RING_SIZE_A>;
using BigRingB  = sprc::Ring<sprc::EncodedPktData, sprc::RING_SIZE_B>;

static sprc::EncodedPktData make_pkt(uint16_t len, uint64_t seq) {
    sprc::EncodedPktData d{};
    d.lenght  = len;
    d.seq_num = seq;
    d.payload[0] = static_cast<uint8_t>(seq & 0xFF);
    return d;
}

static sprc::RawFrameData make_frame(uint16_t len, uint64_t ts) {
    sprc::RawFrameData d{};
    d.length     = len;
    d.arrival_ts = ts;
    d.payload[0] = static_cast<uint8_t>(ts & 0xFF);
    return d;
}

// ── 1. creation ──────────────────────────────────────────────────────────────

static void test_create() {
    section("Ring creation");

    auto ra = BigRingA::create();
    CHECK(ra != nullptr, "Ring A create() returns non-null");

    auto rb = BigRingB::create();
    CHECK(rb != nullptr, "Ring B create() returns non-null");

    auto rs = SmallRing::create();
    CHECK(rs != nullptr, "Small ring create() returns non-null");
}

// ── 2. empty ring ─────────────────────────────────────────────────────────────

static void test_empty_consume() {
    section("Consume on empty ring");

    auto ring = SmallRing::create();

    bool called = false;
    bool ok = ring->consume([&](const sprc::EncodedPktData&) { called = true; });

    CHECK(!ok,     "consume() returns false on empty ring");
    CHECK(!called, "callback not invoked on empty ring");
}

// ── 3. single publish / consume round-trip ────────────────────────────────────

static void test_single_roundtrip() {
    section("Single publish → consume round-trip");

    auto ring = SmallRing::create();
    auto pkt  = make_pkt(42, 7);

    bool pub_ok = ring->publish(pkt);
    CHECK(pub_ok, "publish() returns true on empty ring");

    bool called = false;
    bool con_ok = ring->consume([&](const sprc::EncodedPktData& d) {
        called        = true;
        CHECK(d.lenght  == 42, "  length field preserved");
        CHECK(d.seq_num == 7,  "  seq_num field preserved");
        CHECK(d.payload[0] == (7 & 0xFF), "  payload[0] preserved");
    });

    CHECK(con_ok, "consume() returns true after publish");
    CHECK(called, "callback was invoked");

    // ring should be empty again
    bool empty_ok = ring->consume([](const sprc::EncodedPktData&) {});
    CHECK(!empty_ok, "ring is empty after single consume");
}

// ── 4. full ring — overrun policy ─────────────────────────────────────────────

static void test_ring_full() {
    section("Ring-full overrun policy (drop, no stall)");

    auto ring = SmallRing::create();   // capacity = 8

    // fill all 8 slots
    for (int i = 0; i < 8; ++i) {
        bool ok = ring->publish(make_pkt(static_cast<uint16_t>(i), static_cast<uint64_t>(i)));
        CHECK(ok, "publish into non-full ring succeeds");
    }

    // 9th publish must fail (ring full)
    bool full_ok = ring->publish(make_pkt(99, 99));
    CHECK(!full_ok, "publish returns false when ring is full");

    // consume one to make room
    ring->consume([](const sprc::EncodedPktData&) {});

    // now there is exactly one free slot
    bool after_ok = ring->publish(make_pkt(100, 100));
    CHECK(after_ok, "publish succeeds after consumer frees a slot");
}

// ── 5. wraparound — more than ring_size messages ──────────────────────────────

static void test_wraparound() {
    section("Index wraparound (3× ring capacity)");

    auto ring = SmallRing::create();  // capacity = 8
    constexpr int N = 8 * 3;

    int total_sent     = 0;
    int total_received = 0;
    uint64_t last_seq  = UINT64_MAX;

    for (int i = 0; i < N; ++i) {
        if (ring->publish(make_pkt(1, static_cast<uint64_t>(i)))) {
            ++total_sent;
        }
        ring->consume([&](const sprc::EncodedPktData& d) {
            ++total_received;
            last_seq = d.seq_num;
        });
    }

    CHECK(total_sent     == N, "all messages published through wraparound");
    CHECK(total_received == N, "all messages consumed through wraparound");
    CHECK(last_seq       == static_cast<uint64_t>(N - 1), "last seq_num is correct");
}

// ── 6. data integrity across multiple slots ────────────────────────────────────

static void test_data_integrity() {
    section("Data integrity — payload content");

    auto ring = SmallRing::create();
    constexpr int N = 5;

    for (int i = 0; i < N; ++i) {
        sprc::EncodedPktData d{};
        d.seq_num = static_cast<uint64_t>(i);
        std::memset(d.payload, i, sizeof(d.payload));
        [[maybe_unused]] bool pub_ok = ring->publish(d);
        assert(pub_ok);
    }

    for (int i = 0; i < N; ++i) {
        bool ok = ring->consume([&](const sprc::EncodedPktData& d) {
            CHECK(d.seq_num == static_cast<uint64_t>(i), "seq_num matches publish order");
            bool payload_ok = true;
            for (std::size_t b = 0; b < sizeof(d.payload); ++b) {
                if (d.payload[b] != static_cast<uint8_t>(i)) { payload_ok = false; break; }
            }
            CHECK(payload_ok, "payload bytes match fill value");
        });
        CHECK(ok, "consume succeeds for each published slot");
    }
}

// ── 7. RawFrameData round-trip ────────────────────────────────────────────────

static void test_raw_frame() {
    section("RawFrameData round-trip (Ring A slot type)");

    using SmallRingA = sprc::Ring<sprc::RawFrameData, 8>;
    auto ring = SmallRingA::create();

    auto frame = make_frame(512, 0xDEADBEEF);
    CHECK(ring->publish(frame), "RawFrameData publish succeeds");

    bool ok = ring->consume([&](const sprc::RawFrameData& d) {
        CHECK(d.length     == 512,        "RawFrameData length preserved");
        CHECK(d.arrival_ts == 0xDEADBEEF, "RawFrameData arrival_ts preserved");
        CHECK(d.payload[0] == (0xEF),     "RawFrameData payload[0] preserved");
    });
    CHECK(ok, "RawFrameData consume returns true");
}

// ── 8. threaded SPSC stress ────────────────────────────────────────────────────

static void test_threaded_spsc() {
    section("Threaded SPSC — producer/consumer on separate threads");

    constexpr int N = 100'000;
    auto ring = BigRingB::create();

    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};
    std::atomic<bool> done{false};

    std::thread producer([&] {
        for (int i = 0; i < N; ++i) {
            sprc::EncodedPktData d{};
            d.seq_num = static_cast<uint64_t>(i);
            while (!ring->publish(d)) {
                // spin until consumer makes room
            }
            produced.fetch_add(1, std::memory_order_relaxed);
        }
        done.store(true, std::memory_order_release);
    });

    std::thread consumer([&] {
        uint64_t expected = 0;
        bool seq_ok = true;
        while (!done.load(std::memory_order_acquire) || consumed.load() < N) {
            ring->consume([&](const sprc::EncodedPktData& d) {
                if (d.seq_num != expected) seq_ok = false;
                ++expected;
                consumed.fetch_add(1, std::memory_order_relaxed);
            });
        }
        CHECK(seq_ok, "  all messages received in order");
    });

    producer.join();
    consumer.join();

    CHECK(produced.load() == N, "producer sent all messages");
    CHECK(consumed.load() == N, "consumer received all messages");
}

// ── main ──────────────────────────────────────────────────────────────────────

int main() {
    std::printf("=== sprc::Ring tests ===\n");

    test_create();
    test_empty_consume();
    test_single_roundtrip();
    test_ring_full();
    test_wraparound();
    test_data_integrity();
    test_raw_frame();
    test_threaded_spsc();

    std::printf("\n=== Results: %d passed, %d failed ===\n", g_passed, g_failed);
    return g_failed == 0 ? 0 : 1;
}
