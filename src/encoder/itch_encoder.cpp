#include "encoder/itch_encoder.hpp"
#include "sprc/sprc.hpp"
#include "itch/itch_types.hpp"
#include "itch/mold_header.hpp"
#include <cstring>
#include <string_view>
#include <vector>

namespace {

// Parses "67432.15" directly to integer scaled by 1e8 (= 6743215000000).
// Avoids floating-point entirely — no rounding error, faster than strtod.
uint64_t parse_price(std::string_view s) {
    uint64_t integer_part = 0;
    uint64_t frac_part    = 0;
    int      frac_digits  = 0;
    bool     in_frac      = false;

    for (char c : s) {
        if (c == '.') { in_frac = true; continue; }
        if (in_frac) { frac_part = frac_part * 10 + (c - '0'); ++frac_digits; }
        else         { integer_part = integer_part * 10 + (c - '0'); }
    }

    uint64_t scale = 1;
    for (int i = frac_digits; i < 8; ++i) scale *= 10;

    return integer_part * 100'000'000ULL + frac_part * scale;
}

} // namespace

ItchEncoder::ItchEncoder(std::vector<RingA*> ring_list, RingB& ring_b)
    : ring_list_(std::move(ring_list))
    , ring_b_(ring_b)
    , write_cursor_(pending_slot_.payload + MOLD_HEADER_SIZE)
{
    parsers_[0] = &ItchEncoder::parse_coinbase;
    parsers_[1] = &ItchEncoder::parse_binance;
}

void ItchEncoder::run() {
    for(;;){
        //this needs to be upgraded so that the ring with high volume of data is given more preference
        for(RingA* ring: ring_list_) {
            ring->consume([&](const sprc::RawFrameData& frame){
                process_frame(frame);
            });
        }
        // ring_a_.consume([&](const sprc::RawFrameData& frame) {
        //     process_frame(frame);
        // });
        if (msg_count_ > 0 && (now_ns() - last_flush_ns_) > FLUSH_INTERVAL_NS) flush_slot();

    }
}

void ItchEncoder::flush_slot() {
    if(msg_count_ == 0) return;

    MoldHeader hdr{};
    std::memset(hdr.session, '0', sizeof(hdr.session));
    hdr.seq_num   = first_seq_;
    hdr.msg_count = msg_count_;
    std::memcpy(pending_slot_.payload, &hdr, MOLD_HEADER_SIZE);

    pending_slot_.length = static_cast<uint16_t>(
        write_cursor_ - pending_slot_.payload);

    ring_b_.publish(pending_slot_);

    write_cursor_  = pending_slot_.payload + MOLD_HEADER_SIZE;
    msg_count_     = 0;
    first_seq_     = 0;
    last_flush_ns_ = now_ns();
}

void ItchEncoder::process_frame(const sprc::RawFrameData& frame) {
    if (frame.source_id >= parsers_.size() || !parsers_[frame.source_id]) return;

    std::string_view payload{
        reinterpret_cast<const char*>(frame.payload), frame.length};

    (this->*parsers_[frame.source_id])(payload);
}

void ItchEncoder::parse_coinbase(std::string_view payload) {
    // payload is 
    /*
     * { "channel": "ticker", "events": [ { "tickers": [ { "price": "67432.15", "product_id": "BTC-USD", ... } ] } ] }
       { "channel": "l2_data", "events": [ { "updates": [ { "side": "bid", "price_level": "21921.73", "new_quantity": "0.06" } ] } ] }
     */
    auto doc = sj_parser_.iterate(
        payload.data(),
        payload.size(),
        payload.size() + simdjson::SIMDJSON_PADDING      
    );

    std::string_view channel;
    if(doc["channel"].get(channel) != simdjson::SUCCESS) return;

    if(channel == "ticker"){
        for (auto event : doc["events"].get_array()) {
            for (auto ticker : event["tickers"].get_array()) {
                std::string_view product_id, price_sv, best_bid_sv, best_ask_sv;

                ticker["product_id"].get(product_id);
                ticker["price"].get(price_sv);
                ticker["best_bid"].get(best_bid_sv);
                ticker["best_ask"].get(best_ask_sv);

                const uint64_t price_scaled = parse_price(price_sv);
                const uint64_t bid_scaled   = parse_price(best_bid_sv);
                const uint64_t ask_scaled   = parse_price(best_ask_sv);

                // fill ITCH struct and memcpy into pending_slot_
                PriceTick msg{};
                msg.price    = price_scaled;
                msg.best_bid = bid_scaled;
                msg.best_ask = ask_scaled;
                std::memset(msg.symbol, ' ', 8);
                std::memcpy(msg.symbol, product_id.data(),
                            std::min(product_id.size(), std::size_t{8}));

                std::memcpy(write_cursor_, &msg, sizeof(msg));
                write_cursor_ += sizeof(msg);
                uint64_t s = seq_.fetch_add(1, std::memory_order_relaxed);
                if (msg_count_ == 0) first_seq_ = s;
                msg_count_++;
            }
        }
    } else if (channel == "l2_data") {
        for (auto event : doc["events"].get_array()) {
            for (auto update : event["updates"].get_array()) {
                std::string_view side_sv, price_sv, qty_sv;
                update["side"].get(side_sv);
                update["price_level"].get(price_sv);
                update["new_quantity"].get(qty_sv);

                // qty == "0" means level removed → OrderDelete
                // qty != "0" means level added/updated → AddOrder
                bool is_delete = (qty_sv == "0");

                if (is_delete) {
                    // fill OrderDelete, memcpy into write_cursor_
                } else {
                    AddOrder msg{};
                    msg.side = (side_sv == "bid") ? 'B' : 'S';
                    // ... fill price, qty, symbol same as above
                    std::memcpy(write_cursor_, &msg, sizeof(msg));
                    write_cursor_ += sizeof(msg);
                    uint64_t s = seq_.fetch_add(1, std::memory_order_relaxed);
                    if (msg_count_ == 0) first_seq_ = s;
                    msg_count_++;
                }
            }
        }
    }
    
}

void ItchEncoder::parse_binance(std::string_view payload) {
    // TODO: simdjson parsing
}
