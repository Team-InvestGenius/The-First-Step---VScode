import numpy as np
import pandas as pd
from typing import Literal, Union, List, Dict, Any, Optional
from datetime import datetime
import matplotlib.pyplot as plt
from modules.data.core import DataPipeline
from modules.algo.core import ValueBasedAlgo
from modules.strategy.core import ValueBasedStrategy
from modules.utils import read_config, process_data, parallel_process, prepare_data
from modules.logger import get_logger

logger = get_logger(__name__)


class MomentumStrategy(ValueBasedStrategy):
    def __init__(
        self,
        dps: List[DataPipeline],
        algo: ValueBasedAlgo,
        train_period: int,
        valid_period: int,
        selection_method: Literal["top_n", "threshold", "relative"] = "top_n",
        selection_param: Union[int, float] = 10,  # top_n's default number
        min_stocks: int = 5,
        max_stocks: int = 20,
        **kwargs,
    ):
        super().__init__(dps, algo, train_period, valid_period, **kwargs)
        self.selection_method = selection_method
        self.selection_param = selection_param
        self.min_stocks = min_stocks
        self.max_stocks = max_stocks

        self._data: Optional[pd.DataFrame] = None
        self._selected_stocks: List[str] = []
        self._portfolio_returns: pd.Series = pd.Series()
        self._valid_data: pd.DataFrame = pd.DataFrame()

    def set_data(self):
        """
        It depends on strategy
        :return:

        """
        data_list = parallel_process(process_data, self.dps)
        self._data = prepare_data(data_list)

    def execute(
        self, execute_date: datetime = None, **kwargs
    ) -> Dict[str, Union[List[str], Dict[str, float]]]:

        if self._data is None:
            self.set_data()
        logger.info(f"Executing momentum strategy on {execute_date}")
        logger.info(f"Executing momentum strategy with {self._data.shape} size dataset")

        try:
            self.set_dates(execute_date=execute_date)
            train_data = self._data.loc[self.train_start_date : self.train_end_date]
            self._valid_data = self._data.loc[
                self.valid_start_date: self.valid_end_date
            ]
            prepared_data = self.algo.prepare_data(train_data)
            calculated_values = self.algo.calculate_values(prepared_data)
            self._selected_stocks = self.select_stocks(calculated_values)
            performance = self.calculate_performance()
            return {
                "execute_date": execute_date.strftime("%Y-%m-%d"),
                "selected_stocks": self._selected_stocks,
                "performance": performance,
            }
        except Exception as e:
            logger.error(e)

    def select_stocks(self, calculated_values: pd.Series, **kwargs) -> List[str]:
        if not isinstance(calculated_values, pd.Series):
            raise ValueError(f"Expected a pandas Series, input : {type(calculated_values)}")
        if self.selection_method == "top_n":
            n = np.clip(self.selection_param, self.min_stocks, self.max_stocks)
            selected = calculated_values.nlargest(n)
        elif self.selection_method == "threshold":
            threshold = (
                calculated_values.mean()
                + self.selection_param * calculated_values.std()
            )
            selected = calculated_values[calculated_values > threshold]
            if len(selected) < self.min_stocks:
                selected = calculated_values.nlargest(self.min_stocks)
            elif len(selected) > self.max_stocks:
                selected = selected.nlargest(self.max_stocks)
        elif self.selection_method == "relative":
            market_average = calculated_values.mean()
            selected = calculated_values[calculated_values > market_average]
            if len(selected) < self.min_stocks:
                selected = calculated_values.nlargest(self.min_stocks)
            elif len(selected) > self.max_stocks:
                selected = selected.nlargest(self.max_stocks)
        else:
            raise ValueError("Invalid selection method")

        selected_stocks = selected.index.tolist()

        if not selected_stocks:
            logger.warning(
                f"No stocks were selected using the {self.selection_method} method."
            )
        elif len(selected_stocks) < self.min_stocks:
            logger.info(
                f"Only {len(selected_stocks)} stocks were selected. Using top {self.min_stocks} stocks instead."
            )
            selected_stocks = calculated_values.nlargest(self.min_stocks).index.tolist()
        elif len(selected_stocks) > self.max_stocks:
            logger.info(
                f"{len(selected_stocks)} stocks were selected. Limiting to top {self.max_stocks} stocks."
            )
            selected_stocks = calculated_values.nlargest(self.max_stocks).index.tolist()

        logger.info(
            f"Selected {len(selected_stocks)} stocks: {', '.join(selected_stocks)}"
        )
        return selected_stocks

    def calculate_performance(self) -> Dict[str, float]:
        portfolio = self._valid_data.loc[:, self._selected_stocks]
        returns = portfolio.pct_change().dropna()

        self._portfolio_returns = returns.mean(axis=1)

        cumulative_return = (1 + self._portfolio_returns).prod() - 1
        annual_return = (1 + self._portfolio_returns.mean()) ** 252 - 1
        annual_volatility = self._portfolio_returns.std() * np.sqrt(252)
        sharpe_ratio = (
            np.sqrt(252)
            * self._portfolio_returns.mean()
            / self._portfolio_returns.std()
        )

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
        ax1.plot(cumulative_returns.index, cumulative_returns.values, label="Portfolio")
        ax1.set_title("Cumulative Portfolio Returns")
        ax1.set_ylabel("Cumulative Returns")
        ax1.legend()
        ax1.grid(True)

        # 개별 주식 가격 움직임 그래프
        for stock in self._selected_stocks:
            normalized_price = self._valid_data[stock] / self._valid_data[stock].iloc[0]
            ax2.plot(self._valid_data.index, normalized_price, label=stock)

        ax2.set_title("Individual Stock Price Movements (Normalized)")
        ax2.set_xlabel("Date")
        ax2.set_ylabel("Normalized Price")
        ax2.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
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
