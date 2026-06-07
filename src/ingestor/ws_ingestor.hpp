#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

#include "sprc/sprc.hpp"

// namespace ingestor {
    
    namespace beast = boost::beast;
    namespace net   = boost::asio;
    namespace ssl   = boost::asio::ssl;
    
    using ssl_stream_t = beast::ssl_stream<beast::tcp_stream>;
    using ws_stream_t  = beast::websocket::stream<ssl_stream_t>;
    
    // ── SourceConfig ──────────────────────────────────────────────────────────────
    // All source-specific knowledge lives here.
    // Adding a new data source = write a new factory function that returns a SourceConfig.
    // WsIngestor itself never changes.
    struct SourceConfig {
        std::string host;
        std::string port;         // "443" for standard WSS
        std::string path;         // "/" for Coinbase; Binance encodes streams in the URL
        uint8_t     source_id;    // stamped on every RawFrameData slot; encoder uses it to
                                // pick the right JSON parser (0=coinbase, 1=binance, …)
        std::string source_name;  // used in log output only
    
        // Called once per (re)connect after the WebSocket handshake succeeds.
        // Return one JSON string per message to send (Coinbase needs one per channel;
        // Binance uses URL-based subscriptions so this can return an empty vector).
        std::function<std::vector<std::string>()> build_subscribe_msgs;
    };
    
    // ── WsIngestor ────────────────────────────────────────────────────────────────
    // Generic TLS WebSocket ingestor. Owns the I/O thread's Beast stream and io_context.
    // On connect: TCP → TLS → HTTP upgrade → subscribe → tight read loop → Ring A.
    // On any error: exponential-backoff reconnect, re-sends subscription on recovery.
    class WsIngestor {
    public:
        WsIngestor(SourceConfig cfg,
                sprc::Ring<sprc::RawFrameData, sprc::RING_SIZE_A>& ring);
    
        ~WsIngestor() = default;
        WsIngestor(const WsIngestor&)            = delete;
        WsIngestor& operator=(const WsIngestor&) = delete;
    
        // Blocks forever. Call on a dedicated std::thread.
        void run();
    
        uint64_t dropped_frames() const noexcept {
            return dropped_.load(std::memory_order_relaxed);
        }
    
    private:
        void connect();
        void read_loop();
        void reconnect_with_backoff();
    
        SourceConfig cfg_;
        sprc::Ring<sprc::RawFrameData, sprc::RING_SIZE_A>& ring_;
    
        std::atomic<uint64_t> dropped_{0};
        int backoff_ms_{500};
        static constexpr int MAX_BACKOFF_MS = 30'000;
    
        net::io_context              ioc_;
        ssl::context                 ssl_ctx_;
        beast::flat_buffer           buf_;
        std::unique_ptr<ws_stream_t> ws_;
    };
    
    // ── Factory functions for known sources ───────────────────────────────────────
    // Use these in main.cpp. To add a new source, add a new factory here.
    
    // Coinbase Advanced Trade WebSocket
    // product_ids: e.g. {"BTC-USD", "ETH-USD"}
    // channels:    e.g. {"ticker", "level2", "market_trades"}
    SourceConfig coinbase_config(std::vector<std::string> product_ids,
                                std::vector<std::string> channels);
    
    // Binance combined stream
    // streams: low-level stream names e.g. {"btcusdt@ticker", "ethusdt@bookTicker"}
    // These are appended to wss://stream.binance.com:9443/stream?streams=...
    SourceConfig binance_config(std::vector<std::string> streams);

// }
