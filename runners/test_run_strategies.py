import sys
import os
import glob
import logging
from modules.utils import read_config, create_strategy, create_symbol_mapper
from modules.strategy.strategy_pool import StrategyPool
from modules.strategy.utils import retrieve_selected_stocks
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
    r1_result['trading_preferences'] = "공격투자형"    # 투자 유형

    print(r1_result)
    """
    이걸 그대로 DB에 넣으면 됨
    r1_result의 데이터 유형 Dict 
        { 'strategy_name': 'MomentumStrategy',                  # 전략 이름 
          'execute_date': '2024-07-26',                         # 전략 실행일 
          'selected_stocks': ['138040', '012450', '003670'],    # 선택된 주식
          'selected_stock_names': ['메리츠금융지주', '한화에어로스페이스', '포스코퓨처엠'],        # 선택된 주식 한글 이름 
          'stocks_ratio': {'메리츠금융지주': '33.33%', '한화에어로스페이스': '33.33%', '포스코퓨처엠': '33.33%'},     # 선택된 주식들 비중 
          'annual_return': '12.36%',            # 연평균 기대수수익률
          'annual_volatility': '19.79%',        # 연평균 변동성 
          'sharpe_ratio': '0.59',                # 샤프지수 
          'mdd': '-4.52%',                   # 최대 낙폭 
          'n_of_strategies': 4,             # 해당 전략을 위해 평가한 전체 전략의 수 
          'national': 'KOR',                 # 투자 국가 
          'trading_preferences': '공격투자형'         # 투자 유형 
        } 
    """

    # TODO : db_write(r1_result)  @황경상 or @이성도


    pool2 = StrategyPool(strategies, "conservative")
    r2 = pool2.execute()
    r2_result = retrieve_selected_stocks(r2, symbol_mapper)
    r2_result['n_of_strategies'] = n_of_strategies   # 검토한 전략의 수
    r2_result['national'] = national.upper()         # 투자 국가
    r2_result['trading_preferences'] = "방어투자형"    # 투자 유형

    # TODO : db_write(r2_result)  @황경상 or @이성도

    pool3 = StrategyPool(strategies)  # 위험회피형
    r3 = pool3.execute()
    r3_result = retrieve_selected_stocks(r3, symbol_mapper)
    r3_result['n_of_strategies'] = n_of_strategies   # 검토한 전략의 수
    r3_result['national'] = national.upper()         # 투자 국가
    r3_result['trading_preferences'] = "중립투자형"    # 투자 유형

    # TODO : db_write(r3_result)  @황경상 or @이성도
