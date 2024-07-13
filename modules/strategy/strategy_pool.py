from typing import List
from modules.strategy.strategy import Strategy


class StrategyPool:

    def __init__(
            self,
            strategies: List[Strategy],
            trading_preferences: str,
    ):

        self.strategies = strategies
        self.trading_preferences = trading_preferences

    def execute(self):
        pass
