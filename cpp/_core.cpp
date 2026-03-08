#include <cmath>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
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

PYBIND11_MODULE(_core, m)
{
    m.doc() = "Native C++ extensions for streaming_data";
    m.def("bs_eur_call_price",
          &shawlynot::bs_eur_call_price,
          "Calculate the price of a European call option using the Black-Scholes formula",
          pybind11::arg("spot"),
          pybind11::arg("strike"),
          pybind11::arg("time_to_expiry_years"),
          pybind11::arg("discount_rate"),
          pybind11::arg("vol"));
    m.def("bs_vega",
          &shawlynot::bs_vega,
          "Calculate the Vega of a European call option",
          pybind11::arg("spot"),
          pybind11::arg("strike"),
          pybind11::arg("time_to_expiry_years"),
          pybind11::arg("discount_rate"),
          pybind11::arg("vol"));

    pybind11::class_<shawlynot::MarketDataEventCore>(m, "MarketDataEventCore")
        .def_readonly("option_sec_id", &shawlynot::MarketDataEventCore::option_sec_id)
        .def_readonly("option_price", &shawlynot::MarketDataEventCore::option_price)
        .def_readonly("underlier_price", &shawlynot::MarketDataEventCore::underlier_price)
        .def_readonly("time_nanos", &shawlynot::MarketDataEventCore::time_nanos)
        .def("__repr__", [](const shawlynot::MarketDataEventCore &e)
             { return "MarketDataEventCore(option_sec_id=" + std::to_string(e.option_sec_id) +
                      ", option_price=" + std::to_string(e.option_price) +
                      ", underlier_price=" + std::to_string(e.underlier_price) +
                      ", time_nanos=" + std::to_string(e.time_nanos) + ")"; });

    pybind11::class_<shawlynot::MarketDataState>(m, "MarketDataState")
        .def(pybind11::init<std::unordered_map<int64_t, int64_t>>(), pybind11::arg("underlier_to_option"))
        .def("underlier_tick", &shawlynot::MarketDataState::underlier_tick, pybind11::arg("security_id"), pybind11::arg("price"))
        .def("option_tick",
             &shawlynot::MarketDataState::option_tick,
             pybind11::arg("sec_id"),
             pybind11::arg("price"),
             pybind11::arg("time"));
}