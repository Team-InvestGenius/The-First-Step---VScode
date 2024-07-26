import sys
import os
import glob
from datetime import datetime
import logging
from modules.utils import read_config, create_strategy
from modules.strategy.strategy_pool import StrategyPool
from modules.db.strategy_pool_db import StrategyPoolDB
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
        log_level=logging.WARNING,
        file_level=logging.DEBUG,
        stream_level=logging.INFO,
        # telegram_token과 telegram_chat_id는 필요한 경우 추가
    )

    logger.info("Starting script")

    configs = glob.glob("../configs/strategies/kor/*.yaml")
    strategies = [create_strategy(read_config(config)) for config in configs]
    print(len(strategies))

    pool = StrategyPool(strategies, "aggressive")  # 위험선호형
    r = pool.execute()
    print(r)

    pool2 = StrategyPool(strategies, "conservative") 
    r2 = pool2.execute()
    print(r2)

    pool3 = StrategyPool(strategies, "sharp") 
    r3 = pool3.execute()
    print(r3)

    pool4 = StrategyPool(strategies)  # 위험회피형
    r4 = pool4.execute()
    print(r4)

