import os
import pandas as pd
from typing import Optional
from modules.data.data_pipeline import ProviderDataPipeline
import configparser


def read_config(config_path: str) -> dict:
    """
    설정 파일을 읽어 설정 정보를 반환합니다.
    :param config_path: 설정 파일 경로
    :return: 설정 정보를 담은 딕셔너리
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def load_data(base_path: str, symbol: str, days_back: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    저장된 데이터를 메모리에 로드합니다.
    :param base_path: 데이터를 저장한 기본 경로
    :param symbol: 주식 종목 심볼
    :param days_back: 불러올 데이터의 과거 일수 (옵션)
    :return: 로드된 데이터프레임 (없으면 None 반환)
    """
    symbol_base_path = os.path.join(base_path, symbol)
    pipeline = ProviderDataPipeline(data_provider=None, base_path=symbol_base_path, use_file_lock=True)

    if not os.path.exists(symbol_base_path):
        print(f"No data directory for symbol: {symbol}")
        return None

    if days_back is not None:
        data = pipeline.load(days_back)
    else:
        data = pipeline.get_all_data()

    if data.empty:
        print(f"No data found for symbol: {symbol}")
        return None

    return data


if __name__ == "__main__":
    config_path = 'configs/yahoo_config.ini'
    base_path = 'data'

    # 설정 파일 읽기
    config = read_config(config_path)

    # 각 심볼에 대한 데이터 로드 및 출력
    for section in config.sections():
        symbol = config[section]['symbol']
        days_back = None  # 과거 30일간의 데이터를 로드

        data = load_data(base_path, symbol, days_back)
        if data is not None:
            print(f"Data for {symbol}:\n", data.tail(5))