#include <cmath>
#include <pybind11/pybind11.h>
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

PYBIND11_MODULE(_core, m)
{
    m.doc() = "Native C++ extensions for streaming_data";
    m.def(
        "bs_eur_call_price",
        &shawlynot::bs_eur_call_price,
        "Calculate the price of a European call option using the Black-Scholes formula",
        pybind11::arg("spot"),
        pybind11::arg("strike"),
        pybind11::arg("time_to_expiry_years"),
        pybind11::arg("discount_rate"),
        pybind11::arg("vol"));
    m.def(
        "bs_vega",
        &shawlynot::bs_vega,
        "Calculate the Vega of a European call option",
        pybind11::arg("spot"),
        pybind11::arg("strike"),
        pybind11::arg("time_to_expiry_years"),
        pybind11::arg("discount_rate"),
        pybind11::arg("vol"));
}