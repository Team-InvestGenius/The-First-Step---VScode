import os
import threading
import concurrent.futures
import time
from datetime import datetime
import configparser
from typing import List
from modules.data.yahoo_finance import YahooFinance
from modules.data.data_pipeline import ProviderDataPipeline


def read_config(config_path: str) -> dict:
    """
    설정 파일을 읽어 설정 정보를 반환합니다.
    :param config_path: 설정 파일 경로
    :return: 설정 정보를 담은 딕셔너리
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def create_data_providers(config: dict) -> List[YahooFinance]:
    """
    설정 정보를 바탕으로 데이터 제공자 객체를 생성합니다.
    :param config: 설정 정보 딕셔너리
    :return: 데이터 제공자 객체 리스트
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    providers = []
    for section in config.sections():
        symbol = config[section]['symbol']
        interval = config[section].get('interval', '1d')
        period = config[section].get('period', '1mo')
        start_date = config[section].get('start_date')
        end_date = config[section].get('end_date')
        
        if end_date == 'TODAY':
            end_date = today_str
        
        providers.append(YahooFinance(symbol=symbol, interval=interval, period=period, start_date=start_date, end_date=end_date))
    return providers


def main(config_path: str, base_path: str):
    config = read_config(config_path)
    data_providers = create_data_providers(config)

    # 모든 주식을 한 번씩 가져오기
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(data_providers)) as executor:
        futures = []
        for provider in data_providers:
            symbol_base_path = os.path.join(base_path, provider.symbol)
            pipeline = ProviderDataPipeline(data_provider=provider, base_path=symbol_base_path)
            futures.append(executor.submit(pipeline.fetch_and_save_realtime, threading.Event(), True))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f'Initial fetch generated an exception: {exc}')

    print("Initial fetch for all stocks completed.")

    # 계속해서 데이터 업데이트
    stop_event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(data_providers)) as executor:
        futures = []
        for provider in data_providers:
            symbol_base_path = os.path.join(base_path, provider.symbol)
            pipeline = ProviderDataPipeline(data_provider=provider, base_path=symbol_base_path)
            futures.append(executor.submit(pipeline.fetch_and_save_realtime, stop_event))

        try:
            while True:
                time.sleep(1)  # 메인 스레드가 CPU를 과도하게 사용하지 않도록 함
        except KeyboardInterrupt:
            print("Stopping all tasks...")
            stop_event.set()

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f'Continuous fetch generated an exception: {exc}')

    print("All tasks completed.")


if __name__ == "__main__":
    config_path = 'configs/yahoo_config.ini'
    base_path = 'data'
    main(config_path, base_path)