from .db_connector import DBConnector
import pymysql

class StrategyPoolDB(DBConnector):

    def select(self, where: str = None):
        try:
            with self.db_engine.cursor() as cursor:
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
            with self.db_engine.cursor() as cursor:
                columns = ', '.join(data.keys())
                values = ', '.join(['%s'] * len(data))
                sql = f"INSERT INTO Strategy_pool ({columns}) VALUES ({values});"
                cursor.execute(sql, tuple(data.values()))
                self.db_engine.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing insert: {e}")

    def update(self, data: dict, where: str):
        try:
            with self.db_engine.cursor() as cursor:
                set_clause = ', '.join([f"{key}=%s" for key in data.keys()])
                sql = f"UPDATE Strategy_pool SET {set_clause} WHERE {where};"
                cursor.execute(sql, tuple(data.values()))
                self.db_engine.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing update: {e}")

    def delete(self, where: str):
        try:
            with self.db_engine.cursor() as cursor:
                sql = f"DELETE FROM Strategy_pool WHERE {where};"
                cursor.execute(sql)
                self.db_engine.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing delete: {e}")
