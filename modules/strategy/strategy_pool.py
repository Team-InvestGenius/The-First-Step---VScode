from typing import List
# FIXME : Strategy 전체 클래스 ?
from modules.strategy.core import ValueBasedStrategy
from modules.strategy.strategy import MomentumAlgo


class StrategyPool:

    def __init__(
            self,
            strategies: List[ValueBasedStrategy],
            trading_preferences: str,
    ):

        self.strategies = strategies
        self.trading_preferences = trading_preferences

    def execute(self):

        for strategy in self.strategies:
            strategy.execute()



if __name__ == '__main__':

    config1 = read_config("../../configs/strategies/momentum_strategy1.yaml")
    dp1 = create_pipelines(config1)

    config2 = read_config("../../configs/strategies/momentum_strategy2.yaml")
    dp2 = create_pipelines(config2)

    config3 = read_config("../../configs/strategies/momentum_strategy3.yaml")
    dp3 = create_pipelines(config3)

    algo = MomentumAlgo()

    strategy1 = MomentumStrategy(
        train_start_date="2024-05-01",
        train_end_date="2024-06-30",
        valid_start_date="2024-07-01",
        valid_end_date="2024-07-18",
        dps=dp1,
        algo=algo,
        selection_method=config1["strategy"]["selection_method"],
        selection_param=config1["strategy"]["selection_param"],
    )

    strategy2 = MomentumStrategy(
        train_start_date="2024-05-01",
        train_end_date="2024-06-30",
        valid_start_date="2024-07-01",
        valid_end_date="2024-07-18",
        dps=dp2,
        algo=algo,
        selection_method=config1["strategy"]["selection_method"],
        selection_param=config1["strategy"]["selection_param"],
    )

    strategy3 = MomentumStrategy(
        train_start_date="2024-05-01",
        train_end_date="2024-06-30",
        valid_start_date="2024-07-01",
        valid_end_date="2024-07-18",
        dps=dp3,
        algo=algo,
        selection_method=config1["strategy"]["selection_method"],
        selection_param=config1["strategy"]["selection_param"],
    )

    pool = StrategyPool(
        strategies=[strategy1, strategy2, strategy3],
        trading_preferences=config1["trading"]["trading_preferences"],
    )

    pool.execute()




