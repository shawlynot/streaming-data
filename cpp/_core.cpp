#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <_core.h>

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
