import os
import pandas as pd
from aggregated_dataset_db import AggregatedDatasetDB

def main(data_path: str, db_config: dict):
    """
    데이터 폴더 내의 모든 Yahoo Finance 데이터를 읽어 데이터베이스에 삽입.
    
    :param data_path: Yahoo Finance 데이터가 저장된 폴더 경로
    :param db_config: 데이터베이스 설정 딕셔너리
    """
    # 데이터베이스 연결 설정
    db = AggregatedDatasetDB(
        db_address=db_config['db_address'],
        user_id=db_config['user_id'],
        pw=db_config['pw'],
        db_name=db_config['db_name'],
        port=int(db_config.get('port', 3307))
    )

    # 데이터 폴더 내의 모든 파일을 읽어 데이터베이스에 삽입
    for symbol in os.listdir(data_path):
        symbol_path = os.path.join(data_path, symbol)
        if os.path.isdir(symbol_path):
            for file in os.listdir(symbol_path):
                file_path = os.path.join(symbol_path, file)
                if file.endswith('.csv'):
                    data = pd.read_csv(file_path)
                    db.insert(data, symbol)
                    print(f"Data from {file} inserted into the database.")
    db.close()

if __name__ == "__main__":
    db_config = {
        'db_address': 'project-db-campus.smhrd.com',
        'user_id': 'InvestGenius',
        'pw': '12345',
        'db_name': 'InvestGenius',
        'port': 3307
    }
    data_path = os.path.join(os.path.dirname(__file__), '../../data')
    main(data_path, db_config)
