#include <cmath>
#include <_core.h>
#include <boost/math/distributions/normal.hpp>

const boost::math::normal standard_normal;

double shawlynot::bs_eur_call_price(double spot, double strike, double time_to_expiry_years, double discount_rate, double vol)
{
    double d1 = (std::log(spot / strike) + (discount_rate + 0.5 * vol * vol) * time_to_expiry_years) / (vol * std::sqrt(time_to_expiry_years));
    double d2 = d1 - vol * std::sqrt(time_to_expiry_years);
    return spot * boost::math::cdf(standard_normal, d1) - strike * std::exp(-discount_rate * time_to_expiry_years) * boost::math::cdf(standard_normal, d2);
}

double shawlynot::bs_vega(double spot, double strike, double time_to_expiry_years, double discount_rate, double vol)
{
    double d1 = (std::log(spot / strike) + (discount_rate + 0.5 * vol * vol) * time_to_expiry_years) / (vol * std::sqrt(time_to_expiry_years));
    return spot * boost::math::pdf(standard_normal, d1) * std::sqrt(time_to_expiry_years);
}

shawlynot::MarketDataState::MarketDataState(std::unordered_map<int64_t, int64_t> option_to_underlier)
    : option_to_underlier(std::move(option_to_underlier)) {}

void shawlynot::MarketDataState::underlier_tick(int64_t security_id, int64_t price)
{
    this->underlier_prices[security_id] = price;
}

std::optional<shawlynot::MarketDataEventCore> shawlynot::MarketDataState::option_tick(int64_t sec_id, int64_t price, int64_t time)
{
    this->option_prices[sec_id] = price;

    auto ul_it = this->option_to_underlier.find(sec_id);
    if (ul_it == this->option_to_underlier.end())
        return std::nullopt;

    auto price_it = this->underlier_prices.find(ul_it->second);
    if (price_it == this->underlier_prices.end())
        return std::nullopt;

    return MarketDataEventCore{sec_id, price, price_it->second, time};
}
