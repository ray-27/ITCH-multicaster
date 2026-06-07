#include "sprc/sprc.hpp"
#include "ingestor/ws_ingestor.hpp"
#include <thread>

int main() {
    auto ring_a = sprc::Ring<sprc::RawFrameData, sprc::RING_SIZE_A>::create();

    WsIngestor coinbase(
        coinbase_config(
            {"BTC-USD", "ETH-USD"},          // instruments
            {"ticker", "level2"}             // channels
        ),
        *ring_a
    );

    std::thread t1([&]{ coinbase.run();});

    t1.join();
}
