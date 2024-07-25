import sys
import os

# 현재 파일의 상위 디렉토리를 모듈 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import glob
from datetime import datetime
from modules.utils import read_config, create_strategy
from modules.strategy.strategy_pool import StrategyPool
from modules.db.strategy_pool_db import StrategyPoolDB

def prepare_data_for_insertion(data, strategy_type):
    execute_date = data[0]['execute_date']
    selected_stocks = str(data[0]['selected_stocks'])
    performance = str(data[0]['performance'])
    national = 'KOR'  # 적절한 국가 값을 입력하세요
    return {
        'execute_date': execute_date,
        'trading_preferences': strategy_type,
        'national': national,
        'selected_stocks': selected_stocks,
        'performance': performance
    }

if __name__ == "__main__":
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

    # DBConnector 인스턴스 생성
    db_address = 'project-db-campus.smhrd.com'
    user_id = 'InvestGenius'
    pw = '12345'
    db_name = 'InvestGenius'
    port = 3307

    # StrategyPoolDB 인스턴스 생성
    db = StrategyPoolDB(db_address, user_id, pw, db_name)

    # r 값을 테이블에 삽입
    db.insert(prepare_data_for_insertion(r, "aggressive"))
    db.insert(prepare_data_for_insertion(r2, "conservative"))
    db.insert(prepare_data_for_insertion(r3, "sharp"))
    db.insert(prepare_data_for_insertion(r4, "conservative"))  # 위험회피형을 위해 적절한 값 입력

    # 데이터베이스 연결 닫기
    db.close()
