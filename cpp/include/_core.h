#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <unordered_map>

namespace shawlynot
{
    /**
     * Calculate the price of a European call option using the Black-Scholes formula.
     * @param spot Current price of the underlying asset.
     * @param strike Strike price of the option.
     * @param time_to_expiry_years Time to expiration in years.
     * @param discount_rate Risk-free discount rate.
     * @param vol Annualized volatility of the underlying.
     * @return Theoretical call option price.
     */
    double bs_eur_call_price(double spot, double strike, double time_to_expiry_years, double discount_rate, double vol);

    /**
     * Calculate the vega of a European call option (sensitivity of price to volatility).
     * @param spot Current price of the underlying asset.
     * @param strike Strike price of the option.
     * @param time_to_expiry_years Time to expiration in years.
     * @param discount_rate Risk-free discount rate.
     * @param vol Annualized volatility of the underlying.
     * @return Vega — derivative of option price with respect to volatility.
     */
    double bs_vega(double spot, double strike, double time_to_expiry_years, double discount_rate, double vol);

    struct MarketDataEventCore
    {
        int64_t option_sec_id;
        int64_t option_price;
        int64_t underlier_price;
        int64_t time_nanos;
    };

    class MarketDataState
    {
        std::unordered_map<int64_t, int64_t> underlier_prices;
        std::unordered_map<int64_t, int64_t> option_prices;
        std::unordered_map<int64_t, int64_t> option_to_underlier;

    public:

        /**
         * Construct a MarketDataState with a mapping from underlier to option security IDs.
         * @param underlier_to_option Map from underlier security ID to option security ID.
         */
        MarketDataState(std::unordered_map<int64_t, int64_t> underlier_to_option);

        /**
         * Update the cached price for an underlier on a new tick.
         * @param security_id Security ID of the underlier.
         * @param price Latest underlier price.
         */
        void underlier_tick(int64_t security_id, int64_t price);

        /**
         * Update the cached option price and, if the corresponding underlier price is available,
         * return a MarketDataEventCore joining the option and underlier prices.
         * @param sec_id Security ID of the option.
         * @param underlier_sec_id Security ID of the underlying instrument.
         * @param price Latest option price.
         * @param time Timestamp in nanoseconds.
         * @return A MarketDataEventCore if the underlier price is known, otherwise std::nullopt.
         */
        std::optional<MarketDataEventCore> option_tick(int64_t sec_id, int64_t price, int64_t time);
    };

}