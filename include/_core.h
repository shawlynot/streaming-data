#pragma once

#include <cstdint>
#include <map>
#include <optional>

namespace shawlynot
{
    double bs_eur_call_price(double spot, double strike, double time_to_expiry_years, double discount_rate, double vol);
    double bs_vega(double spot, double strike, double time_to_expiry_years, double discount_rate, double vol);

    struct MarketDataEvent
    {
        int64_t option_sec_id;
        int64_t option_price;
        int64_t underlier_price;
        int64_t time_nanos;
    };

    class MarketDataState
    {
        std::map<int64_t, int64_t> underlier_ticks; // time,price
        std::map<int64_t, std::map<int64_t, int64_t>> option_ticks; // sec_id,(time,price)
        int64_t underlier_sec_id;

    public:
        MarketDataState(int64_t underlier_sec_id);
        std::optional<MarketDataEvent> add_underlier_tick(int64_t price, int64_t time);
        std::optional<MarketDataEvent> add_option_tick(int64_t sec_id, int64_t price, int64_t time);
    };

}