#include "sprc/sprc.hpp"
#include "ingestor/ws_ingestor.hpp"
#include "encoder/itch_encoder.hpp"
#include "multicast/udp_multicaster.hpp"
#include <thread>

int main() {
    auto ring_a = sprc::Ring<sprc::RawFrameData,   sprc::RING_SIZE_A>::create();
    auto ring_b = sprc::Ring<sprc::EncodedPktData, sprc::RING_SIZE_B>::create();

    WsIngestor coinbase(
        coinbase_config({"BTC-USD", "ETH-USD"}, {"ticker", "l2_data"}),
        *ring_a
    );

    ItchEncoder    encoder({ring_a.get()}, *ring_b);
    UDPMulticaster sender(*ring_b, "239.1.1.1", 9000);

    std::thread t1([&]{ coinbase.run(); });
    std::thread t2([&]{ encoder.run();  });
    std::thread t3([&]{ sender.run();   });

    t1.join();
    t2.join();
    t3.join();
}
