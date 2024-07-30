import sys
import os
import pymysql
from modules.db.db_connector import DBConnector
import json

class StrategyDBConnector(DBConnector):

    def select(self, where: str = None):
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM Strategy_pool"
                if where:
                    sql += f" WHERE {where}"
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
        except pymysql.MySQLError as e:
            print(f"Error executing select: {e}")
            return None

    def insert(self, data: dict):
        try:
            with self.connection.cursor() as cursor:
                columns = ", ".join(data.keys())
                values = ", ".join(["%s"] * len(data))
                sql = f"INSERT INTO Strategy_pool ({columns}) VALUES ({values});"
                cursor.execute(sql, tuple(data.values()))
                self.connection.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing insert: {e}")
            self.connection.rollback()

    def update(self, data: dict, where: str):
        try:
            with self.connection.cursor() as cursor:
                set_clause = ", ".join([f"{key}=%s" for key in data.keys()])
                sql = f"UPDATE Strategy_pool SET {set_clause} WHERE {where};"
                cursor.execute(sql, tuple(data.values()))
                self.connection.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing update: {e}")
            self.connection.rollback()

    def delete(self, where: str):
        try:
            with self.connection.cursor() as cursor:
                sql = f"DELETE FROM Strategy_pool WHERE {where};"
                cursor.execute(sql)
                self.connection.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing delete: {e}")
            self.connection.rollback()

    def insert_strategy_result(self, data: dict):
        data = prepare_data(data)
        try:
            with self.connection.cursor() as cursor:
                columns = ", ".join(data.keys())
                values = ", ".join(["%s"] * len(data))
                sql = f"INSERT INTO Strategy_pool ({columns}) VALUES ({values})"
                print(f"Executing SQL: {sql}")
                cursor.execute(sql, tuple(data.values()))
                self.connection.commit()
        except pymysql.MySQLError as e:
            print(f"Error inserting strategy result: {e}")
            self.connection.rollback()

    def get_strategies(self, execute_date, invest_type):
        """
        특정 날짜와 투자 성향에 맞는 전략을 조회합니다.
        :param execute_date: 조회할 날짜 (date 객체)
        :param invest_type: 투자 성향 (string)
        :return: 조회된 전략 목록
        """
        try:
            with self.connection.cursor() as cursor:
                query = """
                    SELECT selected_stocks
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
        종목 심볼 목록에 대한 메타 데이터를 조회합니다.
        :param symbols: 종목 심볼 목록 (list)
        :return: 조회된 메타 데이터 목록
        """
        try:
            with self.connection.cursor() as cursor:
                query = """
                    SELECT National, Symbol, Name, Keywords
                    FROM StockMeta
                    WHERE Symbol IN (%s)
                """ % ",".join(["%s"] * len(symbols))
                cursor.execute(query, tuple(symbols))
                result = cursor.fetchall()
                return result
        except pymysql.MySQLError as e:
            print(f"주식 메타 데이터 가져오기 에러: {e}")
            return None

def prepare_data(data):
    for key, value in data.items():
        if isinstance(value, (list, dict)):
            data[key] = json.dumps(value, ensure_ascii=False)
    return data
