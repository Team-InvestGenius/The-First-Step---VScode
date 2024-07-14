import os
import yaml
import time
import pandas as pd
import concurrent.futures
import threading
import pytz
import importlib
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Callable
from modules.data.data_pipeline import ProviderDataPipeline, DataProvider
from modules.logger import get_logger

logger = get_logger(__name__)

# 상수 정의
CONFIG_KEY_DATA_PIPELINES = "data_pipelines"
CONFIG_KEY_STRATEGY = "strategy"
CONFIG_KEY_STOCKS = "stocks"
CONFIG_KEY_BASE_PATH = "base_path"
CONFIG_KEY_PROVIDER_CLASS = "provider_class"
CONFIG_KEY_STOCKS_FILE = "stocks_file"


def find_project_root(current_path: str) -> str:
    logger.info(f"Searching for project root from: {current_path}")
    while True:
        if os.path.exists(os.path.join(current_path, ".git")):
            logger.info(f"Project root found: {current_path}")
            return current_path
        parent = os.path.dirname(current_path)
        if parent == current_path:
            logger.error("Project root not found")
            raise ValueError("Project root not found")
        current_path = parent


def read_config(config_path: str) -> Dict[str, Any]:
    logger.info(f"Reading config file: {config_path}")
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        logger.debug("Config file loaded successfully")
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise FileNotFoundError(f"Config file not found: {config_path}")
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise ValueError(f"Error parsing YAML file: {e}")

    project_root = find_project_root(os.path.dirname(os.path.abspath(config_path)))

    if CONFIG_KEY_DATA_PIPELINES not in config:
        if (
            CONFIG_KEY_STRATEGY in config
            and CONFIG_KEY_DATA_PIPELINES in config[CONFIG_KEY_STRATEGY]
        ):
            data_pipelines = config[CONFIG_KEY_STRATEGY][CONFIG_KEY_DATA_PIPELINES]
        else:
            data_pipelines = {}
    else:
        data_pipelines = config[CONFIG_KEY_DATA_PIPELINES]

    if CONFIG_KEY_BASE_PATH in data_pipelines:
        data_pipelines[CONFIG_KEY_BASE_PATH] = os.path.normpath(
            os.path.join(project_root, data_pipelines[CONFIG_KEY_BASE_PATH])
        )
    else:
        data_pipelines[CONFIG_KEY_BASE_PATH] = os.path.join(project_root, "data")

    if CONFIG_KEY_STOCKS_FILE in data_pipelines:
        stocks_file = data_pipelines[CONFIG_KEY_STOCKS_FILE]
        stocks_path = os.path.join(os.path.dirname(config_path), stocks_file)
        try:
            with open(stocks_path, "r") as file:
                stocks_config = yaml.safe_load(file)
            data_pipelines[CONFIG_KEY_STOCKS] = stocks_config[CONFIG_KEY_STOCKS]
        except FileNotFoundError:
            logger.warning(
                f"Stocks file '{stocks_file}' not found. Using stocks defined in main config."
            )
        except KeyError:
            logger.warning(
                f"Invalid structure in stocks file '{stocks_file}'. Using stocks defined in main config."
            )

    if CONFIG_KEY_STRATEGY in config:
        config[CONFIG_KEY_STRATEGY][CONFIG_KEY_DATA_PIPELINES] = data_pipelines
    else:
        config[CONFIG_KEY_DATA_PIPELINES] = data_pipelines

    logger.info("Config processing completed")
    return config


def create_data_providers(config: dict) -> List[DataProvider]:
    logger.info("Creating data providers")
    data_pipelines = config[CONFIG_KEY_DATA_PIPELINES]
    stocks = data_pipelines[CONFIG_KEY_STOCKS]

    provider_class_path = data_pipelines[CONFIG_KEY_PROVIDER_CLASS]
    module_name, class_name = provider_class_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    provider_class = getattr(module, class_name)

    logger.info(f"Using provider class: {provider_class.__name__}")

    params = []
    if provider_class.__name__ == "YahooFinance":
        params.extend(["interval", "period", "start_date", "end_date"])
    elif provider_class.__name__ == "FinanceDataReader":
        params.extend(["interval", "start_date", "end_date"])

    providers = []
    for stock in stocks:
        symbol = stock["symbol"]
        provider_params = {
            k: stock.get(k) or data_pipelines.get(k)
            for k in params
        }

        if provider_params.get("end_date") == "TODAY":
            provider_params["end_date"] = datetime.now().strftime("%Y-%m-%d")

        providers.append(provider_class(symbol=symbol, **provider_params))
        logger.debug(f"Created provider for symbol: {symbol}")

    logger.info(f"Created {len(providers)} data providers")
    return providers


def load_data(
    dp: ProviderDataPipeline, n_days_before: Optional[int] = None
) -> Optional[pd.DataFrame]:
    logger.info(f"Loading data for symbol: {dp.data_provider.symbol}")
    try:
        if n_days_before is not None:
            end_date = datetime.now(tz=pytz.UTC).replace(tzinfo=None)
            start_date = end_date - timedelta(days=n_days_before)
            logger.debug(f"Loading data from {start_date} to {end_date}")
            data = dp.get_data_range(start_date, end_date)
        else:
            logger.debug("Loading all available data")
            data = dp.get_all_data()

        if data.empty:
            logger.warning(f"No data found for symbol: {dp.data_provider.symbol}")
            return None
        logger.info(f"Successfully loaded data for symbol: {dp.data_provider.symbol}")
        return data
    except Exception as e:
        logger.error(f"Error loading data for symbol {dp.data_provider.symbol}: {e}")
        return None


def process_data(
    dp: ProviderDataPipeline, n_days_before: Optional[int] = None
) -> Optional[pd.DataFrame]:
    logger.info(f"Processing data for symbol: {dp.data_provider.symbol}")
    try:
        dp.update_to_latest()
        data = load_data(dp, n_days_before)
        if data is not None:
            logger.info(f"Loaded data for {dp.data_provider.symbol}:")
            logger.info(f"Shape: {data.shape}")
            logger.info(f"Date range: {data.index.min()} to {data.index.max()}")
            # 여기에 추가적인 데이터 처리 로직을 구현할 수 있습니다.
        return data
    except Exception as e:
        logger.error(f"Error processing data for symbol {dp.data_provider.symbol}: {e}")
        return None


def create_pipelines(
    config: dict, n_days_before: Optional[int] = None
) -> List[ProviderDataPipeline]:
    logger.info("Creating data pipelines")
    providers = create_data_providers(config)
    base_path = config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH]
    pipelines = []
    for provider in providers:
        symbol_base_path = os.path.join(base_path, provider.symbol)
        pipeline = ProviderDataPipeline(
            data_provider=provider, base_path=symbol_base_path
        )
        pipelines.append(pipeline)
        logger.debug(f"Created pipeline for symbol: {provider.symbol}")

    logger.info(f"Created {len(pipelines)} data pipelines")
    return pipelines


def parallel_process(
    func: Callable, items: List[Any], n_days_before: Optional[int] = None
) -> List[Any]:
    logger.info("Starting parallel processing")
    results = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(32, os.cpu_count() + 4)
    ) as executor:
        futures = [executor.submit(func, item, n_days_before) for item in items]

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                logger.error(f"An error occurred during parallel processing: {e}")

    logger.info("All data processing completed.")
    logger.info(f"Successfully processed {len(results)} items.")

    return results


def run_data_pipeline(config_path: str, base_path: Optional[str] = None):
    logger.info(f"Starting main function with config path: {config_path}")
    config = read_config(config_path)
    if base_path:
        config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH] = base_path
    base_path = config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH]

    data_providers = create_data_providers(config)
    logger.info(f"Created {len(data_providers)} data providers")

    logger.info("Starting initial fetch for all stocks")
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=len(data_providers)
    ) as executor:
        futures = []
        for provider in data_providers:
            symbol_base_path = os.path.join(base_path, provider.symbol)
            pipeline = ProviderDataPipeline(
                data_provider=provider, base_path=symbol_base_path
            )
            futures.append(
                executor.submit(
                    pipeline.fetch_and_save_realtime, threading.Event(), True
                )
            )

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error(
                    f"Initial fetch generated an exception: {exc}", exc_info=True
                )

    logger.info("Initial fetch for all stocks completed.")

    logger.info("Starting continuous data update")
    stop_event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=len(data_providers)
    ) as executor:
        futures = []
        for provider in data_providers:
            symbol_base_path = os.path.join(base_path, provider.symbol)
            pipeline = ProviderDataPipeline(
                data_provider=provider, base_path=symbol_base_path
            )
            futures.append(
                executor.submit(pipeline.fetch_and_save_realtime, stop_event)
            )

        try:
            while True:
                time.sleep(1)  # 메인 스레드가 CPU를 과도하게 사용하지 않도록 함
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt. Stopping all tasks...")
            stop_event.set()

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error(
                    f"Continuous fetch generated an exception: {exc}", exc_info=True
                )

    logger.info("All tasks completed.")
