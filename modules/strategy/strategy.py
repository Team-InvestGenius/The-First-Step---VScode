import numpy as np
import pandas as pd
from typing import Literal, Union, List, Dict, Any, Optional
import matplotlib.pyplot as plt
import seaborn as sns
from modules.data.core import DataPipeline
from modules.algo.core import ValueBasedAlgo
from modules.strategy.core import ValueBasedStrategy
from modules.utils import read_config, process_data, parallel_process
from modules.utils import create_pipelines


class MomentumStrategy(ValueBasedStrategy):
    def __init__(
        self,
        dps: List[DataPipeline],
        algo: ValueBasedAlgo,
        train_start_date: str,
        train_end_date: str,
        valid_start_date: str,
        valid_end_date: str,
        selection_method: Literal["top_n", "threshold", "relative"] = "top_n",
        selection_param: Union[int, float] = 10,
    ):
        super().__init__(
            dps,
            algo,
            train_start_date,
            train_end_date,
            valid_start_date,
            valid_end_date,
        )
        self.selection_method = selection_method
        self.selection_param = selection_param

        self._data: Optional[pd.DataFrame, None] = None
        self._selected_stocks: List[str] = []
        self._portfolio_returns: pd.Series = pd.Series()
        self._valid_data: pd.DataFrame = pd.DataFrame()

    def set_data(self):
        """
        It depends on strategy
        :return:

        """
        data_list = parallel_process(process_data, self.dps)
        all_dfs = []
        for data in data_list:
            for k, df in data.items():
                close_price = df["close"]
                close_price.name = k
                all_dfs.append(close_price)

        all_data = pd.concat(all_dfs, axis=1).dropna()
        self._data = all_data

    def execute(self):
        if self._data is None:
            self.set_data()
        try:
            train_data = self._data.loc[self.train_start_date: self.train_end_date]
            print(train_data.shape)
            self._valid_data = self._data.loc[self.valid_start_date: self.valid_end_date]
            prepared_data = self.algo.prepare_data(train_data)
            calculated_values = self.algo.calculate_values(prepared_data)
            self._selected_stocks = self.select_stocks(calculated_values)
            performance = self.calculate_performance()
            return {"selected_stocks": self._selected_stocks, "performance": performance}
        except Exception as e:
            print(e)

    def select_stocks(self, calculated_values: pd.Series) -> List[str]:
        if not isinstance(calculated_values, pd.Series):
            raise ValueError("Expected a pandas Series")

        if self.selection_method == "top_n":
            selected = calculated_values.nlargest(self.selection_param)
        elif self.selection_method == "threshold":
            threshold = calculated_values.mean() + self.selection_param * calculated_values.std()
            selected = calculated_values[calculated_values > threshold]
        elif self.selection_method == "relative":
            market_average = calculated_values.mean()
            selected = calculated_values[calculated_values > market_average]
        else:
            raise ValueError("Invalid selection method")

        return selected.index.tolist()

    def calculate_performance(self) -> Dict[str, float]:
        portfolio = self._valid_data.loc[:, self._selected_stocks]
        returns = portfolio.pct_change().dropna()

        self._portfolio_returns = returns.mean(axis=1)

        cumulative_return = (1 + self._portfolio_returns).prod() - 1
        annual_return = (1 + self._portfolio_returns.mean()) ** 252 - 1
        annual_volatility = self._portfolio_returns.std() * np.sqrt(252)
        sharpe_ratio = np.sqrt(252) * self._portfolio_returns.mean() / self._portfolio_returns.std()

        cumulative = (1 + self._portfolio_returns).cumprod()
        peak = cumulative.expanding(min_periods=1).max()
        drawdown = (cumulative / peak) - 1
        mdd = drawdown.min()

        return {
            "cumulative_return": cumulative_return,
            "annual_return": annual_return,
            "annual_volatility": annual_volatility,
            "sharpe_ratio": sharpe_ratio,
            "mdd": mdd,
        }

    def plot_backtest(self):
        if self._data is None or not self._selected_stocks:
            raise ValueError("Execute the strategy first before plotting.")

        cumulative_returns = (1 + self._portfolio_returns).cumprod()

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 15), sharex=True)

        # 포트폴리오 누적 수익률 그래프
        ax1.plot(cumulative_returns.index, cumulative_returns.values, label='Portfolio')
        ax1.set_title('Cumulative Portfolio Returns')
        ax1.set_ylabel('Cumulative Returns')
        ax1.legend()
        ax1.grid(True)

        # 개별 주식 가격 움직임 그래프
        for stock in self._selected_stocks:
            normalized_price = self._valid_data[stock] / self._valid_data[stock].iloc[0]
            ax2.plot(self._valid_data.index, normalized_price, label=stock)

        ax2.set_title('Individual Stock Price Movements (Normalized)')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Normalized Price')
        ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax2.grid(True)

        plt.tight_layout()
        plt.show()

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params.update(
            {
                "selection_method": self.selection_method,
                "selection_param": self.selection_param,
            }
        )
        return params


if __name__ == "__main__":

    # 전략 부분 전체적으로 수정 필요!!

    config = read_config("../../configs/strategies/momentum_strategy3.yaml")
    dps = create_pipelines(config)

    # 전략 초기화
    from modules.algo.momentum import MomentumAlgo

    algo = MomentumAlgo()

    strategy = MomentumStrategy(
        train_start_date="2024-05-01",
        train_end_date="2024-06-30",
        valid_start_date="2024-07-01",
        valid_end_date="2024-07-18",
        dps=dps,
        algo=algo,
        selection_method=config["strategy"]["selection_method"],
        selection_param=config["strategy"]["selection_param"],
    )

    p = strategy.execute()

    print(p)
    strategy.plot_backtest()