import os
import configparser
from datetime import datetime
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


def create_yahoo_providers(config: dict,
                          default_interval: str = '1d',
                          default_period: str = 'max') -> List[YahooFinance]:

    today_str = datetime.now().strftime('%Y-%m-%d')
    providers = []
    for section in config.sections():
        symbol = config[section]['symbol']
        interval = config[section].get('interval', default_interval)
        period = config[section].get('period', default_period)
        start_date = config[section].get('start_date')
        end_date = config[section].get('end_date')

        if end_date == 'TODAY':
            end_date = today_str

        providers.append(
            YahooFinance(symbol=symbol, interval=interval, period=period, start_date=start_date, end_date=end_date))
    return providers


def create_pipelines(config: dict, base_path: str) -> List[ProviderDataPipeline]:

    providers = create_yahoo_providers(config)
    pipelines = []
    for provider in providers:
        symbol_base_path = os.path.join(base_path, provider.symbol)
        pipeline = ProviderDataPipeline(
            data_provider=provider, base_path=symbol_base_path)
        pipelines.append(pipeline)

    return pipelines





