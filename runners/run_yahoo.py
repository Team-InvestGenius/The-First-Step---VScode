import os
import sys
import logging
from modules.utils import run_data_pipeline, read_config
from modules.logger import get_logger, setup_global_logging

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 로거 설정
logger = get_logger(__name__)

if __name__ == "__main__":
    # 전역 로깅 설정
    setup_global_logging(
        log_dir=os.path.join(project_root, "logs"),
        log_level=logging.INFO,
        file_level=logging.DEBUG,
        stream_level=logging.INFO,
        # telegram_token과 telegram_chat_id는 필요한 경우 추가
    )

    logger.info("Starting script")
    config_path = os.path.join(
        project_root, "configs", "datapipelines", "yahoo_config.yaml"
    )

    # config 파일 읽기
    config = read_config(config_path)

    # provider_class가 설정되어 있지 않다면 YahooFinance로 설정
    if 'provider_class' not in config['data_pipelines']:
        config['data_pipelines']['provider_class'] = 'modules.data.yahoo_finance.YahooFinance'
        logger.info(f"Provider class set to: {config['data_pipelines']['provider_class']}")

    run_data_pipeline(config_path)
    logger.info("Script completed")