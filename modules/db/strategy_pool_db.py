import pymysql
from db_connector import DBConnector

class StrategyDBConnector(DBConnector):

        def get_strategies(self, execute_date, invest_type):
             
            """
            특정 날짜와 투자 성향에 맞는 전략을 조회합니다.
            :param execute_date: 조회할 날짜 (date 객체)
            :param invest_type: 투자 성향 (string)
            :return: 조회된 전략 목록
            """
            try:
                with self.db_engine.cursor() as cursor:
                    query = """
                    SELECT  selected_stocks
                    FROM Strategy_pool 
                    WHERE DATE(execute_date) = %s AND invest_type = %s
                    """
                    cursor.execute(query, (execute_date, invest_type))
                    result = cursor.fetchall()
                    return result
            except pymysql.MySQLError as e:
                print(f"전략 가져오기 에러: {e}")
                return None

        def get_stock_meta(self, symbols):
                
            """
             종목 심볼 목록에 대한 메타 데이터를 조회
            :param symbols: 종목 심볼 목록 (list)
            :return: 조회된 메타 데이터 목록
            """
            try:
                with self.db_engine.cursor() as cursor:
                    query = """
                    SELECT National, Symbol, Name, Keywords
                    FROM StockMeta
                    WHERE Symbol IN (%s)
                    """ % ','.join(['%s'] * len(symbols))
                    cursor.execute(query, tuple(symbols))
                    result = cursor.fetchall()
                    return result
            except pymysql.MySQLError as e:
                print(f"주식 메타 데이터 가져오기 에러: {e}")
                return None
