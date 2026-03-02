from streaming_data.model import MarketDataEvent, TickEvent

class MarketDataHandler:
    
    def on_tick(self, tick: TickEvent) -> MarketDataEvent | None:
        # maintain market state in python, then move to C++ std::map and use arrow as an interchange?
        # current state
        # 1) underlyer price stored in a red black tree sorted by time
        # 2) redblack trees for each option tree
        # Events:
        # 1) underlyer tick: add to tree. Check for any matches in option trees. If match (closest option tick within 5 mins?), emit event?
        # 2) option tick: add to tree. Check for matches in underlyer tree. If match (closest market tick within 5 mins?)
        # 
        pass