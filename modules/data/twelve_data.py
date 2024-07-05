import requests
import pandas as pd
from typing import Optional


class TwelveData:
    API_ADDRESS = "https://api.twelvedata.com/time_series"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_data(
        self,
        symbol: str,
        interval: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        country: str = "US",
        exchange: Optional[str] = None,
        type: str = "stock"
    ) -> Optional[pd.DataFrame]:

        params = {
            "apikey": self.api_key,
            "symbol": symbol,
            "interval": interval,
            "country": country,
            "type": type
        }

        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if exchange:
            params["exchange"] = exchange

        try:
            response = requests.get(self.API_ADDRESS, params=params)
            response.raise_for_status()
            data = response.json()

            if 'values' in data:
                df = pd.DataFrame(data['values'])
                df['datetime'] = pd.to_datetime(df['datetime'])
                df = df.set_index('datetime')
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                return df
            else:
                print(f"API 응답에 'values' 키가 없습니다. 응답: {data}")
                return None

        except requests.RequestException as e:
            print(f"API 호출 중 오류 발생: {e}")
            return None

    def print_data(self, df: Optional[pd.DataFrame]):
        if df is not None and not df.empty:
            print(df)
        else:
            print("사용 가능한 데이터가 없습니다.")

    def save_to_csv(self, df: Optional[pd.DataFrame], filename: str):
        if df is not None and not df.empty:
            df.to_csv(filename)
            print(f"데이터가 {filename}에 저장되었습니다.")
        else:
            print("저장할 데이터가 없습니다.")


