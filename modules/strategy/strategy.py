import numpy as np
import pandas as pd
from typing import Literal, Union, List, Dict, Any
from modules.data.core import DataPipeline
from modules.algo.core import ValueBasedAlgo
from modules.strategy.core import ValueBasedStrategy


class MomentumStrategy(ValueBasedStrategy):
    def __init__(
        self,
        train_start_date: str,
        train_end_date: str,
        valid_start_date: str,
        valid_end_date: str,
        dps: List[DataPipeline],
        algo: ValueBasedAlgo,
        selection_method: Literal["top_n", "threshold", "relative"] = "top_n",
        selection_param: Union[int, float] = 10,
    ):
        super().__init__(train_start_date, train_end_date, valid_start_date, valid_end_date, dps, algo)
        self.selection_method = selection_method
        self.selection_param = selection_param

        self._data: Union[pd.DataFrame, None] = None

    def set_data(self):
        """
        It depends on strategy
        :return:
        """
        dfs = []
        symbols = []
        for dp in self.dps:
            if dp.data_provider is None:
                raise Exception("No data provider")
            else:
                symbol = dp.data_provider.symbol
                data = dp.get_all_data()["close"]  # get close price
                symbols.append(symbol)
                dfs.append(data)

        all_data = pd.concat(dfs, axis=1).dropna()
        all_data.columns = symbols
        self._data = all_data

    def select_stocks(self, calculated_values: pd.DataFrame) -> List[str]:
        latest_data = calculated_values.loc[self.train_end_date]
        # 데이터 쪼개서 학습, 검증하는거 확인....
        if self.selection_method == "top_n":
            selected = latest_data.nlargest(self.selection_param)
        elif self.selection_method == "threshold":
            threshold = latest_data.mean() + self.selection_param * latest_data.std()
            selected = latest_data[latest_data > threshold]
        elif self.selection_method == "relative":
            market_average = latest_data.mean()
            selected = latest_data[latest_data > market_average]
        else:
            raise ValueError("Invalid selection method")

        return selected.index.tolist()

    def calculate_performance(self, data: pd.DataFrame, selected_stocks: List[str]) -> Dict[str, float]:
        portfolio = data.loc[self.valid_start_date : self.valid_end_date, selected_stocks]
        returns = portfolio.pct_change().dropna()

        portfolio_returns = returns.mean(axis=1)
        cumulative_return = (1 + portfolio_returns).prod() - 1
        volatility = portfolio_returns.std() * np.sqrt(252)
        sharpe_ratio = (portfolio_returns.mean() * 252) / volatility

        cumulative = (1 + portfolio_returns).cumprod()
        peak = cumulative.expanding(min_periods=1).max()
        drawdown = (cumulative / peak) - 1
        mdd = drawdown.min()

        return {
            "cumulative_return": cumulative_return,
            "volatility": volatility,
            "sharpe_ratio": sharpe_ratio,
            "mdd": mdd,
        }

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params.update({
            "selection_method": self.selection_method,
            "selection_param": self.selection_param
        })
        return params



if __name__ == "__main__":

    from modules.utils import read_config
    from modules.utils import create_yahoo_providers
    from modules.utils import create_pipelines
    from modules.algo.momentum import MomentumAlgo

    config = read_config("../../configs/strategies/momentum_strategy.yaml")
    pipelines = create_pipelines(config)

    # 전략 초기화
    from modules.algo.momentum import MomentumAlgo

    algo = MomentumAlgo()

    strategy = MomentumStrategy(
        train_start_date=config['strategy']['train_start_date'],
        train_end_date=config['strategy']['train_end_date'],
        valid_start_date=config['strategy']['valid_start_date'],
        valid_end_date=config['strategy']['valid_end_date'],
        dps=pipelines,
        algo=algo,
        selection_method=config['strategy']['selection_method'],
        selection_param=config['strategy']['selection_param'],
    )