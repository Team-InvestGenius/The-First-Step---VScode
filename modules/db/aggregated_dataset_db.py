from db_connector import DBConnector
import pandas as pd
import pymysql

class AggregatedDatasetDB(DBConnector):

    def select(self, where: str = None):
        """
        AggregatedDataset 테이블에서 모든 데이터를 조회하고 반환.
        
        :param where: 선택적 조건 문자열
        :return: 조회된 데이터의 리스트
        """
        try:
            with self.db_engine.cursor() as cursor:
                sql = "SELECT * FROM AggregatedDataset"
                if where:
                    sql += f" WHERE {where}"
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
        except pymysql.MySQLError as e:
            print(f"Error executing select: {e}")
            return None

    def insert(self, data, symbol):
        """
        Yahoo Finance 데이터를 AggregatedDataset 테이블에 삽입.
        
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
            try:
                with self.db_engine.cursor() as cursor:
                    columns = ', '.join(record.keys())
                    values = ', '.join(['%s'] * len(record))
                    sql = f"INSERT INTO AggregatedDataset ({columns}) VALUES ({values});"
                    cursor.execute(sql, tuple(record.values()))
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
