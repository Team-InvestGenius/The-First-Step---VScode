import pandas as pd
from typing import Optional, List
from modules.algo.core import ValueBasedAlgo


class MovingAverage(ValueBasedAlgo):

    def __init__(
        self,
        train_start_date: str,
        train_end_date: str,
        valid_start_date: str,
        valid_end_date: str,
        indicator_type: str = "ma",
        **kwargs
    ):
        super().__init__(
            train_start_date,
            train_end_date,
            valid_start_date,
            valid_end_date,
            indicator_type,
            **kwargs
        )

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        pass

    def calculate_values(self, data: pd.DataFrame) -> pd.DataFrame:
        pass

    def select_stocks(self, calculated_values: pd.DataFrame) -> List[str]:
        pass
