from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict
from typing import Any


class ValueBasedAlgo(ABC):
    def __init__(self, indicator_type: str, **kwargs):
        self.indicator_type = indicator_type
        self.params = kwargs

    @abstractmethod
    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 전처리 메서드
        :param data: 원본 데이터
        :return: 전처리된 데이터
        """
        pass

    @abstractmethod
    def calculate_values(self, data: pd.DataFrame) -> pd.Series:
        """
        지표 값 계산 메서드
        :param data: 전처리된 데이터
        :return: 계산된 지표 값
        """
        pass

    def get_params(self) -> Dict[str, Any]:
        """
        알고리즘 파라미터 반환 메서드
        :return: 알고리즘 파라미터 딕셔너리
        """
        return {
            "indicator_type": self.indicator_type,
            **self.params
        }

    def set_params(self, **params):
        """
        알고리즘 파라미터 설정 메서드
        :param params: 설정할 파라미터
        """
        self.params.update(params)

    def predict(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        알고리즘 실행 메서드
        :param data: 입력 데이터
        :return: 계산된 지표 값
        """
        prepared_data = self.prepare_data(data)
        return self.calculate_values(prepared_data)
