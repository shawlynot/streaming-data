import argparse
import logging
from datetime import datetime

from streaming_data.tick_replay import replay_ticks

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    args = parser.parse_args()
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    replay_ticks(start)
