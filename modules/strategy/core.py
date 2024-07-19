from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Union, Any
from modules.data.core import DataPipeline
from modules.algo.core import ValueBasedAlgo


class ValueBasedStrategy(ABC):
    def __init__(
        self,
        dps: List[DataPipeline],
        algo: ValueBasedAlgo,
        train_start_date: str,
        train_end_date: str,
        valid_start_date: str,
        valid_end_date: str,
        **kwargs
    ):
        self._dps = dps
        self._algo = algo
        self.train_start_date = train_start_date
        self.train_end_date = train_end_date
        self.valid_start_date = valid_start_date
        self.valid_end_date = valid_end_date
        self.additional_params = kwargs

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

    @abstractmethod
    def select_stocks(self, calculated_values: pd.DataFrame, **kwargs) -> List[str]:
        pass

    @abstractmethod
    def calculate_performance(self, **kwargs) -> Dict[str, float]:
        pass

    def update_dps(self):
        for dp in self.dps:
            dp.update_to_latest()

    @abstractmethod
    def execute(self, **kwargs) -> Dict[str, Union[List[str], Dict[str, float]]]:
        pass

    def get_params(self) -> Dict[str, Any]:
        params = {
            "dps": {dp.data_provider.symbol: dp.get_params() for dp in self.dps},
            "algo": self.algo.get_params(),
            "train_start_date": self.train_start_date,
            "train_end_date": self.train_end_date,
            "valid_start_date": self.valid_start_date,
            "valid_end_date": self.valid_end_date,
        }
        params.update(self.additional_params)
        return params

    def set_params(self, **params):
        for key, value in params.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                self.additional_params[key] = value
        if "algo" in params:
            self.algo.set_params(**params["algo"])