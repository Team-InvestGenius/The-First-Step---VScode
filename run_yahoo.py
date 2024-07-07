import concurrent.futures
import configparser
import pandas as pd
from typing import List
import os
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
    providers = []
    for section in config.sections():
        symbol = config[section]['symbol']
        providers.append(YahooFinance(symbol=symbol, interval="1d", period="max"))
    return providers


def main(config_path: str, base_path: str):
    """
    메인 함수로, 설정 파일을 읽어 데이터 수집 파이프라인을 실행합니다.
    :param config_path: 설정 파일 경로
    :param base_path: 데이터를 저장할 기본 경로
    """
    # 설정 파일 읽기
    config = read_config(config_path)
    data_providers = create_data_providers(config)

    # 스레드 풀을 사용하여 데이터 수집 병렬 처리
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for provider in data_providers:
            symbol_base_path = os.path.join(base_path, provider.symbol)
            pipeline = ProviderDataPipeline(data_provider=provider, base_path=symbol_base_path)
            futures.append(executor.submit(pipeline.fetch_and_save_realtime))

        # 완료된 작업 처리
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f'Generated an exception: {exc}')


if __name__ == "__main__":
    config_path = 'configs/yahoo_config.ini'
    base_path = 'data'
    main(config_path, base_path)