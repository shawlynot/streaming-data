# shawlynot streaming-data

Just some experiments with Python and C++

## Structure
```
/app # python and c++ with uv and sickit-build
/infra # ansible scripts for deploying this on my Hetzner box (currently broken)
```

## TODO

1. Read historical american NVDA call options and historical spot from Massive ✅
    1. Do I need ref data? Yes ✅
2. Ignore dividends and calculate implied vols assuming European options ✅
3. Profile and optimise. Maybe some c++ extensions in Python ✅
4. Consider dividends and price using a binomial model
5. Crowbar in some ML?