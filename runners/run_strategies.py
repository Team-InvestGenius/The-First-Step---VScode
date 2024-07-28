import sys
import os
import glob
import logging

# 현재 파일의 상위 디렉토리 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, 'modules'))
sys.path.append(os.path.join(project_root, 'modules', 'db'))

from modules.utils import read_config, create_strategy, create_symbol_mapper
from modules.strategy.strategy_pool import StrategyPool
from modules.strategy.utils import retrieve_selected_stocks
from modules.logger import get_logger, setup_global_logging
from modules.db.strategy_pool_db import StrategyDBConnector

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

    national = "kor"

    config_file_list = glob.glob(f"../configs/strategies/{national}/*.yaml")
    configs = [read_config(config_file) for config_file in config_file_list]
    strategies = [create_strategy(config) for config in configs]

    symbol_mapper = create_symbol_mapper(configs)
    n_of_strategies = len(strategies)

    pool = StrategyPool(strategies, "aggressive")  # 위험선호형
    r = pool.execute()

    r1_result = retrieve_selected_stocks(r, symbol_mapper)
    r1_result['n_of_strategies'] = n_of_strategies   # 검토한 전략의 수
    r1_result['national'] = national.upper()         # 투자 국가
    r1_result['invest_type'] = "공격투자형"          # 투자 유형

    print(r1_result)
    
    db_address = 'project-db-campus.smhrd.com'
    user_id = 'InvestGenius'
    pw = '12345'
    db_name = 'InvestGenius'
    port = 3307

    db = StrategyDBConnector()
    db.insert_strategy_result(r1_result)
    db.close()

    pool2 = StrategyPool(strategies, "conservative")
    r2 = pool2.execute()
    r2_result = retrieve_selected_stocks(r2, symbol_mapper)
    r2_result['n_of_strategies'] = n_of_strategies   # 검토한 전략의 수
    r2_result['national'] = national.upper()         # 투자 국가
    r2_result['invest_type'] = "방어투자형"           # 투자 유형

    db = StrategyDBConnector()
    db.insert_strategy_result(r2_result)
    db.close()

    pool3 = StrategyPool(strategies)  # 위험회피형
    r3 = pool3.execute()
    r3_result = retrieve_selected_stocks(r3, symbol_mapper)
    r3_result['n_of_strategies'] = n_of_strategies   # 검토한 전략의 수
    r3_result['national'] = national.upper()         # 투자 국가
    r3_result['invest_type'] = "중립투자형"           # 투자 유형

    db = StrategyDBConnector()
    db.insert_strategy_result(r3_result)
    db.close()
