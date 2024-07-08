import pymysql
import pandas as pd

class DataBase:

    def __init__(self, db_address, user_id, pw, db_name, port=3307):
        """
        데이터베이스 연결 설정.
        
        :param db_address: 데이터베이스 주소
        :param user_id: 데이터베이스 사용자 ID
        :param pw: 데이터베이스 비밀번호
        :param db_name: 데이터베이스 이름
        :param port: 데이터베이스 포트 (기본값 3307)
        """
        self.db_address = db_address
        self.user_id = user_id
        self.pw = pw
        self.db_name = db_name
        self.port = port
        self.db_engine = self.connect_to_db()

    def connect_to_db(self):
        """
        데이터베이스에 연결하고 연결 객체를 반환.
        
        :return: 데이터베이스 연결 객체
        """
        try:
            connection = pymysql.connect(
                host=self.db_address,
                user=self.user_id,
                password=self.pw,
                db=self.db_name,
                port=self.port,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            return connection
        except pymysql.MySQLError as e:
            print(f"Error connecting to the database: {e}")
            return None

    def select(self):
        """
        AggregatedDataset 테이블에서 첫 10개의 데이터를 조회하고 반환.
        
        :return: 조회된 데이터의 리스트
        """
        try:
            with self.db_engine.cursor() as cursor:
                sql = "SELECT * FROM AggregatedDataset LIMIT 10;"
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
        except pymysql.MySQLError as e:
            print(f"Error executing select: {e}")
            return None

    def insert(self, data: dict):
        """
        AggregatedDataset 테이블에 데이터를 삽입.
        
        :param data: 삽입할 데이터 딕셔너리
        """
        try:
            with self.db_engine.cursor() as cursor:
                columns = ', '.join(data.keys())
                values = ', '.join(['%s'] * len(data))
                sql = f"INSERT INTO AggregatedDataset ({columns}) VALUES ({values});"
                cursor.execute(sql, tuple(data.values()))
                self.db_engine.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing insert: {e}")

    def update(self, data: dict, where: str):
        """
        AggregatedDataset 테이블의 특정 조건에 맞는 데이터를 업데이트.
        
        :param data: 업데이트할 데이터 딕셔너리
        :param where: 업데이트 조건 문자열
        """
        try:
            with self.db_engine.cursor() as cursor:
                set_clause = ', '.join([f"{key}=%s" for key in data.keys()])
                sql = f"UPDATE AggregatedDataset SET {set_clause} WHERE {where};"
                cursor.execute(sql, tuple(data.values()))
                self.db_engine.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing update: {e}")

    def delete(self, where: str):
        """
        AggregatedDataset 테이블의 특정 조건에 맞는 데이터를 삭제.
        
        :param where: 삭제 조건 문자열
        """
        try:
            with self.db_engine.cursor() as cursor:
                sql = f"DELETE FROM AggregatedDataset WHERE {where};"
                cursor.execute(sql)
                self.db_engine.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing delete: {e}")

def insert_yahoo_finance_data(db, data, symbol):
    """
    Yahoo Finance 데이터를 AggregatedDataset 테이블에 삽입.
    
    :param db: 데이터베이스 객체
    :param data: Yahoo Finance 데이터 DataFrame
    :param symbol: 심볼 이름
    """
    if data.empty:
        print(f"No data found for {symbol}")
        return

    data['date'] = pd.to_datetime(data['date'])  # date 컬럼을 datetime 타입으로 변환

    for index, row in data.iterrows():
        record = {
            'asset_id': index + 1,  # 자동 증가 값 대신 인덱스 + 1 사용
            'trade_date': row['date'].date(),
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume'],
            'exchange': 'NASDAQ',  # 고정값으로 설정
            'asset_type': 'EQUITY',  # 고정값으로 설정
            'national': 'US',  # 고정값으로 설정
            'asset_name': symbol  # 심볼명 설정
        }
        db.insert(record)
