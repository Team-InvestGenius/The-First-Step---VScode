import pytz
import pandas as pd
from abc import ABC, abstractmethod
from pandas.tseries.offsets import BDay
from typing import List, Dict, Union, Any
from datetime import datetime
from modules.data.core import DataPipeline
from modules.algo.core import ValueBasedAlgo


class ValueBasedStrategy(ABC):
    def __init__(
        self,
        dps: List[DataPipeline],
        algo: ValueBasedAlgo,
        train_period: int,  # num of days
        valid_period: int,  # num of datys
        **kwargs
    ):
        self._dps = dps
        self._algo = algo
        self._train_period = train_period
        self._valid_period = valid_period

        self.additional_params = kwargs

        self._execute_date = None
        self._train_start_date = None
        self._train_end_date = None
        self._valid_start_date = None
        self._valid_end_date = None

    @property
    def dps(self) -> List[DataPipeline]:
        return self._dps

    @dps.setter
    def dps(self, dps: List[DataPipeline]):
        self._dps = dps

    @property
    def algo(self) -> ValueBasedAlgo:
        return self._algo

    @algo.setter
    def algo(self, algo: ValueBasedAlgo):
        self._algo = algo

    @property
    def train_period(self) -> int:
        return self._train_period

    @train_period.setter
    def train_period(self, value: int):
        self._train_period = value
        if self._execute_date:
            self.set_dates(self._execute_date)

    @property
    def valid_period(self) -> int:
        return self._valid_period

    @valid_period.setter
    def valid_period(self, value: int):
        self._valid_period = value
        if self._execute_date:
            self.set_dates(self._execute_date)

    @property
    def execute_date(self) -> datetime:
        return self._execute_date

    @execute_date.setter
    def execute_date(self, value: datetime):
        self._execute_date = value
        self.set_dates(value)

    @property
    def train_start_date(self) -> datetime:
        return self._train_start_date

    @property
    def train_end_date(self) -> datetime:
        return self._train_end_date

    @property
    def valid_start_date(self) -> datetime:
        return self._valid_start_date

    @property
    def valid_end_date(self) -> datetime:
        return self._valid_end_date

    @abstractmethod
    def select_stocks(self, calculated_values: pd.DataFrame, **kwargs) -> List[str]:
        pass

    @abstractmethod
    def calculate_performance(self, **kwargs) -> Dict[str, float]:
        pass

    def update_dps(self):
        for dp in self.dps:
            dp.update_to_latest()

    def set_dates(self, execute_date: datetime = None):
        if execute_date is None:
            execute_date = datetime.now(tz=pytz.UTC)
        self._execute_date = execute_date

        self._valid_end_date = self._execute_date - BDay(1)
        self._valid_start_date = self._valid_end_date - BDay(self._valid_period)
        self._train_end_date = self._valid_start_date - BDay(1)
        self._train_start_date = self._train_end_date - BDay(self._train_period)

        if (
            self._train_start_date >= self._train_end_date
            or self._valid_start_date >= self._valid_end_date
        ):
            raise ValueError(
                "Invalid date ranges. Please check train_period and valid_period."
            )

    @abstractmethod
    def execute(
        self, execute_date: datetime = None, **kwargs
    ) -> Dict[str, Union[List[str], Dict[str, float]]]:
        pass

    def get_params(self) -> Dict[str, Any]:
        params = {
            "dps": {dp.data_provider.symbol: dp.get_params() for dp in self.dps},
            "algo": self.algo.get_params(),
            "train_period": self.train_period,
            "valid_period": self.valid_period,
            "execute_date": self.execute_date,
        }
        params.update(self.additional_params)
        return params

    def set_params(self, **params):
        date_related_params = ["train_period", "valid_period", "execute_date"]
        date_params_changed = False

        for key, value in params.items():
            if key in date_related_params:
                setattr(self, key, value)
                date_params_changed = True
            elif hasattr(self, key):
                setattr(self, key, value)
            else:
                self.additional_params[key] = value

        if "algo" in params:
            self.algo.set_params(**params["algo"])

        if date_params_changed:
            self.set_dates(self.execute_date)