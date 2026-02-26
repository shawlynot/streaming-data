#pragma once

namespace shawlynot
{
    double bs_eur_call_price(double spot, double strike, double time_to_expiry_years, double discount_rate, double vol);
    double bs_vega(double spot, double strike, double time_to_expiry_years, double discount_rate, double vol);
}