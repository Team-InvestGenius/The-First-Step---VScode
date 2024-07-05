import os
import time
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
from modules.data.twelve_data import TwelveData


class DataPipeline:
    def __init__(self, twelve_data: TwelveData, symbol: str, interval: str, country: str = "US",
                 exchange: Optional[str] = None):
        self.twelve_data = twelve_data
        self.symbol = symbol
        self.interval = interval
        self.country = country
        self.exchange = exchange
        self.data = pd.DataFrame()

    def fetch_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        df = self.twelve_data.get_data(
            symbol=self.symbol,
            interval=self.interval,
            start_date=start_date,
            end_date=end_date,
            country=self.country,
            exchange=self.exchange
        )
        if df is not None and not df.empty:
            self.data = pd.concat([self.data, df]).drop_duplicates().sort_index()
        return df

    def fetch_start(self, days: int = 30):
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        return self.fetch_data(start_date=start_date, end_date=end_date)

    def fetch_end(self, last_date: Optional[str] = None):
        if last_date is None and not self.data.empty:
            last_date = self.data.index[-1].strftime("%Y-%m-%d")
        elif last_date is None:
            raise ValueError("마지막 날짜를 지정하거나 데이터를 먼저 가져와야 합니다.")

        return self.fetch_data(start_date=last_date)

    def continuous_fetch(self, interval_seconds: int = 300):
        while True:
            print(f"{datetime.now()}: 데이터 가져오는 중...")
            self.fetch_end()
            self.save_data()
            time.sleep(interval_seconds)

    def save_data(self, filename: Optional[str] = None):
        if filename is None:
            filename = f"{self.symbol}_{self.interval}_data.csv"
        self.twelve_data.save_to_csv(self.data, filename)

    def print_latest_data(self, rows: int = 10):
        self.twelve_data.print_data(self.data.tail(rows))


