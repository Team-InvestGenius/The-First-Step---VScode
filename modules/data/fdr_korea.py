import FinanceDataReader as fdr
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta
from modules.data.core import DataProvider


class FinanceDataReader(DataProvider):
    """
    get yahoo finance data
    default time zone : UTC
    """

    def __init__(
        self,
        symbol: str,
        interval: str = "1D",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        self.symbol = symbol
        self.interval = interval
        super().__init__(start_date=start_date, end_date=end_date)

    def get_data(self) -> pd.DataFrame:

        params = {
            "symbol": self.symbol,
        }

        if self.start_date:
            params["start"] = self.start_date
        if self.end_date:
            params["end"] = self.end_date

        try:
            df = fdr.DataReader(**params)

            if df.empty:
                print(f"No data found for {self.symbol}")
                return pd.DataFrame()

            df = df.reset_index()
            df.columns = df.columns.str.lower()
            df = df.rename(columns={"date": "datetime"})
            df = df.set_index("datetime")

            # 1. tz information이 없기 때문에 Asia/Seoul 기준으로 tz 셋업
            # 2. 그 다음 change UTC
            df.index = df.index.tz_localize('Asia/Seoul')
            df.index = df.index.tz_convert('UTC')

            return df

        except Exception as e:
            print(f"Error fetching data for {self.symbol}: {e}")
            return pd.DataFrame()

    def ping(self) -> bool:
        try:
            end_date = self.end_date or datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')
            df = fdr.DataReader(self.symbol, start=start_date, end=end_date)
            return not df.empty
        except Exception:
            return False

