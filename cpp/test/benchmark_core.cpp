#include <benchmark/benchmark.h>
#include <_core.h>

static void BM_bs_eur_call_price(benchmark::State& state) {
  for (auto _ : state)
        double price = shawlynot::bs_eur_call_price(100.0, 100.0, 1.0, 0.05, 0.2);
}
// Register the function as a benchmark
BENCHMARK(BM_bs_eur_call_price);

BENCHMARK_MAIN();