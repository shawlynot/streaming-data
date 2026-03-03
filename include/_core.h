#pragma once

#include <cstdint>
#include <map>

namespace shawlynot
{
    double bs_eur_call_price(double spot, double strike, double time_to_expiry_years, double discount_rate, double vol);
    double bs_vega(double spot, double strike, double time_to_expiry_years, double discount_rate, double vol);

    class MarketData
    {
    private:
        std::map<uint64_t, int64_t> underlier_ticks;
        std::map<uint64_t, std::map<uint64_t, int64_t>> option_ticks;
    // TODO ing
    };
}