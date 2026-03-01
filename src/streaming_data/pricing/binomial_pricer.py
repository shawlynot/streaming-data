import logging

from ..model import TickConsumer


logger = logging.getLogger(__name__)

class BinomialPricer(TickConsumer):
    
    def on_tick(self, tick):
        logger.info(tick)