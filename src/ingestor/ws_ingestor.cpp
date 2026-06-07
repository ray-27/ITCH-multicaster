#include "ingestor/ws_ingestor.hpp"

#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

#include <openssl/err.h>
#include <time.h>

// namespace ingestor {

    namespace beast = boost::beast;
    namespace net   = boost::asio;
    namespace ssl   = boost::asio::ssl;
    using tcp       = net::ip::tcp;

    namespace {
        // clock_gettime is cheaper than std::chrono on Linux (~20 ns vs ~60 ns).
        inline uint64_t now_ns() noexcept {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL
                + static_cast<uint64_t>(ts.tv_nsec);
        }

    }

    // ── WsIngestor ────────────────────────────────────────────────────────────────

    WsIngestor::WsIngestor(SourceConfig cfg,
                        sprc::Ring<sprc::RawFrameData, sprc::RING_SIZE_A>& ring)
        : cfg_(std::move(cfg))
        , ring_(ring)
        , ssl_ctx_(ssl::context::tlsv12_client)
    {
        ssl_ctx_.set_default_verify_paths();
        ssl_ctx_.set_verify_mode(ssl::verify_peer);
    }

    void WsIngestor::connect() {
        ws_.reset();
        buf_.clear();

        // ── Resolve ──────────────────────────────────────────────────────────────
        tcp::resolver resolver{ioc_};
        auto const results = resolver.resolve(cfg_.host, cfg_.port);

        ws_ = std::make_unique<ws_stream_t>(ioc_, ssl_ctx_);

        // ── TCP connect ──────────────────────────────────────────────────────────
        beast::get_lowest_layer(*ws_).expires_after(std::chrono::seconds(30));
        beast::get_lowest_layer(*ws_).connect(results);

        // ── SNI — required by Coinbase and most CDN-backed endpoints ─────────────
        if (!SSL_set_tlsext_host_name(ws_->next_layer().native_handle(),
                                    cfg_.host.c_str())) {
            throw beast::system_error{
                beast::error_code(static_cast<int>(::ERR_get_error()),
                                net::error::get_ssl_category()),
                "SSL_set_tlsext_host_name"};
        }

        // ── TLS handshake ────────────────────────────────────────────────────────
        beast::get_lowest_layer(*ws_).expires_after(std::chrono::seconds(30));
        ws_->next_layer().handshake(ssl::stream_base::client);

        // Read loop must not timeout — we want ws_->read() to block indefinitely
        beast::get_lowest_layer(*ws_).expires_never();

        // ── HTTP upgrade → WebSocket ─────────────────────────────────────────────
        ws_->set_option(beast::websocket::stream_base::decorator(
            [this](beast::websocket::request_type& req) {
                req.set(beast::http::field::host,       cfg_.host);
                req.set(beast::http::field::user_agent, "itch-multicast/1.0");
            }));
        ws_->handshake(cfg_.host, cfg_.path);

        // ── Subscribe ────────────────────────────────────────────────────────────
        for (const auto& msg : cfg_.build_subscribe_msgs()) {
            ws_->write(net::buffer(msg));
        }

        backoff_ms_ = 500; // reset to minimum on successful connect
        std::cerr << "[" << cfg_.source_name << "] connected\n";
    }

    void WsIngestor::read_loop() {
        sprc::RawFrameData slot{};
        slot.source_id = cfg_.source_id;

        for (;;) {
            ws_->read(buf_); // blocks until a complete WebSocket frame arrives

            const std::size_t n = buf_.size();

            slot.arrival_ts = now_ns();
            slot.length     = static_cast<uint16_t>(
                                std::min(n, sizeof(slot.payload)));

            // buffer_copy handles multi-segment flat_buffer correctly
            net::buffer_copy(net::buffer(slot.payload, slot.length), buf_.data());
            buf_.consume(buf_.size());

            if (!ring_.publish(slot)) [[unlikely]]
                dropped_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void WsIngestor::reconnect_with_backoff() {
        std::cerr << "[" << cfg_.source_name << "] reconnecting in "
                << backoff_ms_ << " ms\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms_));
        backoff_ms_ = std::min(backoff_ms_ * 2, MAX_BACKOFF_MS);
    }

    void WsIngestor::run() {
        for (;;) {
            try {
                connect();
                read_loop();
            } catch (const beast::system_error& e) {
                // A clean remote close is not an error worth logging loudly
                if (e.code() != beast::websocket::error::closed)
                    std::cerr << "[" << cfg_.source_name << "] ws error: "
                            << e.what() << "\n";
            } catch (const std::exception& e) {
                std::cerr << "[" << cfg_.source_name << "] exception: "
                        << e.what() << "\n";
            }
            reconnect_with_backoff();
        }
    }

    // ── Factory implementations ───────────────────────────────────────────────────

    SourceConfig coinbase_config(std::vector<std::string> product_ids,
                                std::vector<std::string> channels) {
        return SourceConfig{
            .host        = "advanced-trade-ws.coinbase.com",
            .port        = "443",
            .path        = "/",
            .source_id   = 0,
            .source_name = "coinbase",

            // Coinbase requires one subscribe message per channel.
            // All product_ids can be bundled into a single message per channel.
            .build_subscribe_msgs = [pids = std::move(product_ids),
                                    chs  = std::move(channels)]() {
                std::vector<std::string> msgs;
                msgs.reserve(chs.size());

                for (const auto& ch : chs) {
                    std::string ids_json;
                    for (std::size_t i = 0; i < pids.size(); ++i) {
                        if (i) ids_json += ',';
                        ids_json += '"' + pids[i] + '"';
                    }
                    msgs.push_back(
                        R"({"type":"subscribe","channel":")" + ch +
                        R"(","product_ids":[)" + ids_json + "]}");
                }
                return msgs;
            }
        };
    }

    SourceConfig binance_config(std::vector<std::string> streams) {
        // Binance combined stream encodes subscriptions in the URL path:
        // wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker
        std::string path = "/stream?streams=";
        for (std::size_t i = 0; i < streams.size(); ++i) {
            if (i) path += '/';
            path += streams[i];
        }

        return SourceConfig{
            .host        = "stream.binance.com",
            .port        = "9443",
            .path        = std::move(path),
            .source_id   = 1,
            .source_name = "binance",
            // No post-handshake subscribe message needed — streams are in the URL
            .build_subscribe_msgs = []() { return std::vector<std::string>{}; }
        };
    }

// }
