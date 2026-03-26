#include <gtest/gtest.h>
#include <_core.h>
#include <cmath>

// --- Black-Scholes pricing tests ---

TEST(BsEurCallPrice, AtTheMoneyCall)
{
    double price = shawlynot::bs_eur_call_price(100.0, 100.0, 1.0, 0.05, 0.2);
    // ATM call with moderate vol should be roughly 10-11
    EXPECT_NEAR(price, 10.4506, 0.001);
}

TEST(BsEurCallPrice, DeepOutOfTheMoney)
{
    double price = shawlynot::bs_eur_call_price(50.0, 100.0, 1.0, 0.05, 0.2);
    // Deep OTM call should be near zero
    EXPECT_NEAR(price, 0.0, 0.01);
}

TEST(BsEurCallPrice, ShortExpiry)
{
    double price = shawlynot::bs_eur_call_price(100.0, 100.0, 0.01, 0.05, 0.2);
    // Very short expiry ATM call should be small
    EXPECT_GT(price, 0.0);
    EXPECT_LT(price, 2.0);
}

TEST(BsEurCallPrice, PutCallParity)
{
    double spot = 100.0, strike = 105.0, T = 0.5, r = 0.03, vol = 0.25;
    double call = shawlynot::bs_eur_call_price(spot, strike, T, r, vol);
    // C = S - K*e^(-rT) + P  =>  C >= S - K*e^(-rT)
    double forward_discount = strike * std::exp(-r * T);
    EXPECT_GE(call, std::max(0.0, spot - forward_discount));
}

// --- Vega tests ---

TEST(BsVega, PositiveVega)
{
    double vega = shawlynot::bs_vega(100.0, 100.0, 1.0, 0.05, 0.2);
    EXPECT_GT(vega, 0.0);
}

TEST(BsVega, AtTheMoneyMaxVega)
{
    // Vega is highest ATM
    double vega_atm = shawlynot::bs_vega(100.0, 100.0, 1.0, 0.05, 0.2);
    double vega_otm = shawlynot::bs_vega(100.0, 130.0, 1.0, 0.05, 0.2);
    double vega_itm = shawlynot::bs_vega(100.0, 70.0, 1.0, 0.05, 0.2);
    EXPECT_GT(vega_atm, vega_otm);
    EXPECT_GT(vega_atm, vega_itm);
}

TEST(BsVega, VegaIncreasesWithExpiry)
{
    double vega_short = shawlynot::bs_vega(100.0, 100.0, 0.25, 0.05, 0.2);
    double vega_long = shawlynot::bs_vega(100.0, 100.0, 1.0, 0.05, 0.2);
    EXPECT_GT(vega_long, vega_short);
}

TEST(BsVega, KnownValue)
{
    double vega = shawlynot::bs_vega(100.0, 100.0, 1.0, 0.05, 0.2);
    EXPECT_NEAR(vega, 37.524, 0.01);
}

// --- MarketDataState tests ---

TEST(MarketDataState, OptionTickWithoutUnderlierReturnsNullopt)
{
    // option 10 maps to underlier 1
    std::unordered_map<int64_t, int64_t> mapping = {{10, 1}};
    shawlynot::MarketDataState state(mapping);
    auto result = state.option_tick(10, 500, 1000);
    EXPECT_FALSE(result.has_value());
}

TEST(MarketDataState, OptionTickWithUnderlierReturnsEvent)
{
    std::unordered_map<int64_t, int64_t> mapping = {{10, 1}};
    shawlynot::MarketDataState state(mapping);
    state.underlier_tick(1, 10000);
    auto result = state.option_tick(10, 500, 1000);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->option_sec_id, 10);
    EXPECT_EQ(result->option_price, 500);
    EXPECT_EQ(result->underlier_price, 10000);
    EXPECT_EQ(result->time_nanos, 1000);
}

TEST(MarketDataState, UnknownOptionReturnsNullopt)
{
    std::unordered_map<int64_t, int64_t> mapping = {{10, 1}};
    shawlynot::MarketDataState state(mapping);
    state.underlier_tick(1, 10000);
    // option 99 is not in the mapping
    auto result = state.option_tick(99, 500, 1000);
    EXPECT_FALSE(result.has_value());
}

TEST(MarketDataState, UnderlierPriceUpdates)
{
    std::unordered_map<int64_t, int64_t> mapping = {{10, 1}};
    shawlynot::MarketDataState state(mapping);
    state.underlier_tick(1, 10000);
    state.underlier_tick(1, 11000);
    auto result = state.option_tick(10, 500, 2000);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->underlier_price, 11000);
}

TEST(MarketDataState, MultipleOptionsShareUnderlier)
{
    // Two options mapping to the same underlier
    std::unordered_map<int64_t, int64_t> mapping = {{10, 1}, {20, 1}};
    shawlynot::MarketDataState state(mapping);
    state.underlier_tick(1, 10000);

    auto r1 = state.option_tick(10, 500, 1000);
    auto r2 = state.option_tick(20, 700, 2000);

    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r1->option_sec_id, 10);
    EXPECT_EQ(r2->option_sec_id, 20);
    EXPECT_EQ(r1->underlier_price, 10000);
    EXPECT_EQ(r2->underlier_price, 10000);
}

TEST(MarketDataState, IndependentUnderliers)
{
    std::unordered_map<int64_t, int64_t> mapping = {{10, 1}, {20, 2}};
    shawlynot::MarketDataState state(mapping);
    state.underlier_tick(1, 10000);
    // underlier 2 has no price yet
    auto r1 = state.option_tick(10, 500, 1000);
    auto r2 = state.option_tick(20, 700, 2000);

    ASSERT_TRUE(r1.has_value());
    EXPECT_FALSE(r2.has_value());
}
