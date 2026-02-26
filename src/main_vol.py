import logging
from streaming_data.vol.call_vol import get_vol_call


logger = logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    get_vol_call()
