import cProfile
import logging
import pstats
import sys
from streaming_data.vol.call_vol import get_vol_call

logger = logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    pr = cProfile.Profile()
    pr.enable()
    get_vol_call()
    pr.disable()
    ps = pstats.Stats(pr, stream=sys.stdout).sort_stats("cumtime")
    ps.print_stats(20)
