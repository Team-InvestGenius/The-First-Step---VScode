import pytz
from datetime import datetime
from typing import List, Optional, Dict, Tuple, Union
from modules.strategy.core import ValueBasedStrategy


def filter_portfolios(portfolios: List[Tuple[Dict, ValueBasedStrategy]]) -> List[
    Tuple[Dict, ValueBasedStrategy]]:
    return [
        (portfolio, strategy) for portfolio, strategy in portfolios
        if portfolio['performance']['cumulative_return'] > 0
           and portfolio['performance']['sharpe_ratio'] > 0
        # additional filtering here
    ]


class StrategyPool:

    def __init__(
            self,
            strategies: List[ValueBasedStrategy],
            trading_preferences: Optional[str] = None,
    ):
        self.strategies = strategies
        self.trading_preferences = trading_preferences or 'balanced'

    def execute(self, execute_date: datetime = None) -> Tuple[Dict, ValueBasedStrategy]:
        if execute_date is None:
            execute_date = datetime.now(tz=pytz.UTC)

        portfolios = []
        for strategy in self.strategies:
            portfolios.append((strategy.execute(execute_date), strategy))

        return self.select_best_portfolio(portfolios)

    def select_best_portfolio(self, portfolios: List[Tuple[Dict, ValueBasedStrategy]]) -> Union[
        Tuple[Dict, ValueBasedStrategy], str]:
        filtered_portfolios = filter_portfolios(portfolios)

        if not filtered_portfolios:
            return "No suitable strategy found based on the given criteria."

        if self.trading_preferences == 'aggressive':
            return max(filtered_portfolios, key=lambda x: x[0]['performance']['cumulative_return'])
        elif self.trading_preferences == 'conservative':
            return min(filtered_portfolios, key=lambda x: x[0]['performance']['mdd'])
        elif self.trading_preferences == 'sharp':
            return max(filtered_portfolios, key=lambda x: x[0]['performance']['sharpe_ratio'])
        elif self.trading_preferences == 'low_volatility':
            return min(filtered_portfolios, key=lambda x: x[0]['performance']['annual_volatility'])
        else:  # balanced (default)
            return max(filtered_portfolios, key=lambda x: x[0]['performance']['sharpe_ratio'])

