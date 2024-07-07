import yfinance as yf
import pandas as pd
from typing import Optional
from modules.data.core import DataProvider


class YahooFinance(DataProvider):
    """
    get yahoo finance data
    default time zone : UTC
    """

    def __init__(
        self,
        symbol: str,
        interval: str,
        period: str,
        raise_errors: bool = True,
        keepna: bool = True,
        timeout: int = 100,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        self.symbol = symbol
        self.interval = interval
        self.period = period
        self.raise_errors = raise_errors
        self.keepna = keepna
        self.timeout = timeout
        super().__init__(start_date=start_date, end_date=end_date)

    def get_data(self) -> pd.DataFrame:

        ticker = yf.Ticker(self.symbol)

        params = {
            "period": self.period,
            "interval": self.interval,
            "raise_errors": self.raise_errors,
            "keepna": self.keepna,
            "timeout": self.timeout,
            "prepost": True,
        }

        if self.start_date:
            params["start"] = self.start_date
        if self.end_date:
            params["end"] = self.end_date

        try:
            """
            history default params
            period="1mo", interval="1d",
            start=None, end=None, prepost=False, actions=True,
            auto_adjust=True, back_adjust=False, repair=False, keepna=False,
            proxy=None, rounding=False, timeout=10,
            raise_errors=False) -> pd.DataFrame:
            """

            df = ticker.history(**params)

            if df.empty:
                print(f"No data found for {self.symbol}")
                return pd.DataFrame()

            df = df.reset_index()
            df = df.rename(columns={"Date": "datetime", "Volume": "volume"})
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.drop_duplicates(subset=["datetime"], keep="last")
            df["datetime"] = df["datetime"].dt.tz_convert(
                "UTC"
            )  # Set timezone to UTC, Summer time zone 도 고려 가능
            df = df.set_index("datetime")

            # 소문자로 컬럼명 변경
            df.columns = df.columns.str.replace(" ", "_")
            df.columns = df.columns.str.lower()

            return df

        except Exception as e:
            print(f"Error fetching data for {self.symbol}: {e}")
            return pd.DataFrame()

    def ping(self) -> bool:
        try:
            yf.Ticker(self.symbol).info
            return True
        except Exception:
            return False
