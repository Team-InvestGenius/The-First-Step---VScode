from abc import ABCMeta, abstractmethod
import os
import pandas as pd
from typing import Optional
from typing import Dict
from typing import Any
from datetime import datetime, timedelta
from filelock import FileLock
from contextlib import nullcontext


class DataProvider(metaclass=ABCMeta):

    def __init__(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ):
        self._start_date = start_date
        self._end_date = end_date

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date

    @start_date.setter
    def start_date(self, start_date):
        self._start_date = start_date

    @end_date.setter
    def end_date(self, end_date):
        self._end_date = end_date

    @abstractmethod
    def get_data(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def ping(self) -> bool:
        pass


class DataPipeline(metaclass=ABCMeta):
    def __init__(
        self,
        data_provider: DataProvider,
        base_path: str,
        use_file_lock: bool = True,
        cache_days: int = 7,
    ):
        self.data_provider = data_provider
        self.base_path = base_path
        self.use_file_lock = use_file_lock
        self.cache_days = cache_days
        os.makedirs(base_path, exist_ok=True)
        self._cached_data = (
            self._load_cache() if data_provider is None else pd.DataFrame()
        )

    def get_params(self) -> Dict[str, Any]:
        return {
            "data_provider": self.data_provider if self.data_provider else "None",
            "base_path": self.base_path,
            "use_file_lock": self.use_file_lock,
            "cache_days": self.cache_days,
        }

    @abstractmethod
    def fetch_data(self, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def fetch_start(self, **kwargs):
        pass

    def _load_cache(self) -> pd.DataFrame:
        end_date = pd.Timestamp.now(tz="UTC").date()
        start_date = end_date - timedelta(days=self.cache_days)
        return self._load_date_range(start_date, end_date)

    def _load_date_range(
        self, start_date: datetime.date, end_date: datetime.date
    ) -> pd.DataFrame:
        all_data = [
            self._read_csv(self._get_file_path(current_date.date(), chunk_num))
            for current_date in pd.date_range(start_date, end_date, freq="MS")
            for chunk_num in range(1000)
            if os.path.exists(self._get_file_path(current_date.date(), chunk_num))
        ]
        return pd.concat(all_data) if all_data else pd.DataFrame()

    def _get_file_path(self, date: datetime.date, chunk_num: int = 0) -> str:
        month_start = date.replace(day=1)
        return os.path.join(self.base_path, f"{month_start}_chunk{chunk_num}.csv")

    def _read_csv(self, file_path: str) -> pd.DataFrame:
        with (
            FileLock(file_path + ".lock", timeout=60)
            if self.use_file_lock
            else nullcontext()
        ):
            data = pd.read_csv(file_path)
        if "date" in data.columns:
            data["date"] = pd.to_datetime(data["date"], utc=True)
            return data.set_index("date")
        else:
            print(f"'date' column not found in {file_path}")
            return pd.DataFrame()

    def _save_data(self, data: pd.DataFrame):
        chunk_num = 0
        while not data.empty:
            chunk_data = data.iloc[: self.chunk_size]
            data = data.iloc[self.chunk_size :]

            file_path = self._get_file_path(chunk_data.index[0].date(), chunk_num)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with (
                FileLock(file_path + ".lock", timeout=60)
                if self.use_file_lock
                else nullcontext()
            ):
                chunk_data.to_csv(
                    file_path,
                    mode="a",
                    header=not os.path.exists(file_path),
                    index=True,
                )
            chunk_num += 1

    def get_all_data(self) -> pd.DataFrame:
        if not os.path.exists(self.base_path):
            print(f"No data directory at {self.base_path}")
            return pd.DataFrame()

        all_data = [
            self._read_csv(os.path.join(root, file))
            for root, _, files in os.walk(self.base_path)
            for file in files
            if file.endswith(".csv")
        ]
        return pd.concat(all_data).sort_index().drop_duplicates(keep="last") if all_data else pd.DataFrame()

    def get_data_range(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        all_data = self.get_all_data()
        if all_data.empty:
            print(f"No data found in the range {start_date} to {end_date}")
            return pd.DataFrame()

        if start_date:
            all_data = all_data[all_data.index >= pd.Timestamp(start_date, tz="UTC")]
        if end_date:
            all_data = all_data[all_data.index <= pd.Timestamp(end_date, tz="UTC")]
        return all_data

    def get_latest_n_days(self, n: int) -> pd.DataFrame:
        end_date = pd.Timestamp.now(tz="UTC")
        start_date = end_date - timedelta(days=n)
        return self.get_data_range(start_date, end_date)

    def clean_old_data(self, days: int):
        cutoff_date = pd.Timestamp.now(tz="UTC").date() - timedelta(days=days)
        for root, dirs, _ in os.walk(self.base_path):
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                try:
                    dir_date = datetime.strptime(dir, "%Y-%m-%d").date()
                    if dir_date < cutoff_date:
                        for file in os.listdir(dir_path):
                            os.remove(os.path.join(dir_path, file))
                        os.rmdir(dir_path)
                except ValueError:
                    continue  # 날짜 형식이 아닌 디렉토리는 무시

    def get_latest_date(self) -> Optional[datetime.date]:
        """get latest date in each symbol"""
        all_data = self.get_all_data()
        if not all_data.empty:
            return all_data.index.max().date()
        return None

    def update_to_latest(self):
        """각 날짜별로 최신 데이터로 업데이트합니다."""
        print("update")
        if self.data_provider is None:
            print("데이터 제공자가 설정되지 않았습니다.")
            return

        latest_date = self.get_latest_date()
        if latest_date is None:
            print("데이터가 없습니다. 전체 데이터를 가져옵니다.")
            self.fetch_data()
        else:
            current_date = pd.Timestamp.now(tz="UTC").date()
            while latest_date < current_date:
                latest_date += timedelta(days=1)
                self.data_provider.start_date = latest_date
                new_data = self.fetch_data()
                if new_data.empty:
                    break
                print(f"{latest_date} 데이터를 업데이트했습니다.")

    def save(self):
        self._save_data(self._cached_data)

    def load(self, days_back: int = 30) -> pd.DataFrame:
        end_date = pd.Timestamp.now(tz="UTC").date()
        start_date = end_date - timedelta(days=days_back)
        return self.get_data_range(start_date, end_date)

