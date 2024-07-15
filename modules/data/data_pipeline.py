import time
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta
from modules.data.core import DataProvider
from modules.data.core import DataPipeline


class ProviderDataPipeline(DataPipeline):
    def __init__(
        self,
        data_provider: DataProvider,
        base_path: str,
        use_file_lock: bool = True,
        cache_days: int = 7,
        fetch_interval: int = 60,
        chunk_size: int = 10000,
    ):
        """
        실시간 데이터 파이프라인 초기화
        :param data_provider: 데이터 제공자 객체
        :param base_path: 데이터를 저장할 기본 경로
        :param use_file_lock: 파일 잠금 사용 여부
        :param cache_days: 메모리에 캐시할 날짜 수
        :param fetch_interval: 데이터 가져오기 간격 (초)
        :param chunk_size: 데이터를 저장할 청크 크기
        """
        super().__init__(data_provider, base_path, use_file_lock, cache_days)
        self.fetch_interval = fetch_interval
        self.chunk_size = chunk_size
        self._current_date = pd.Timestamp.now(tz="UTC").date()

    def fetch_data(self, **kwargs) -> pd.DataFrame:
        """새로운 데이터를 가져와 저장하고 캐시를 업데이트합니다."""
        if self.data_provider is None:
            return pd.DataFrame()

        self._cached_data = self.get_all_data()
        new_data = self.data_provider.get_data()

        if not new_data.empty:
            new_data["date"] = pd.to_datetime(new_data.index, utc=True)
            new_data = new_data.set_index("date")

            if not self._cached_data.empty:
                last_date = self._cached_data.index.max()
                new_data = new_data[new_data.index > last_date]

            if not new_data.empty:
                self._save_data(new_data)
                self._cached_data = pd.concat([self._cached_data, new_data]).sort_index()
                cutoff_date = pd.Timestamp.now(tz="UTC") - timedelta(days=self.cache_days)
                self._cached_data = self._cached_data.loc[self._cached_data.index >= cutoff_date]

        return new_data

    def fetch_and_save_realtime(self, stop_event, single_fetch=False):
        while not stop_event.is_set():
            current_date = pd.Timestamp.now(tz="UTC").date()
            if current_date != self._current_date:
                print(f"날짜가 바뀌었습니다. 새로운 폴더에 데이터를 저장합니다: {current_date}")
                self._current_date = current_date

            new_data = self.fetch_data()

            if not new_data.empty:
                self._save_data(new_data)
                print(f"{self.data_provider.symbol}: 새로운 데이터 {len(new_data)}행이 저장되었습니다.")
            else:
                print(f"{self.data_provider.symbol}: 새로운 데이터가 없습니다.")

            if single_fetch:
                break

            time.sleep(self.fetch_interval)

    def fetch_start(self, **kwargs):
        """데이터 가져오기를 시작합니다."""
        self._cached_data = self.get_all_data()
        if self._cached_data.empty:
            self._cached_data = self.data_provider.get_data()
        else:
            self.data_provider.start_date = self._cached_data.index.max()