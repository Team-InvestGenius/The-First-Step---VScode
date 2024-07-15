import pymysql
from abc import ABC, abstractmethod
import pandas as pd

class DBConnector(ABC):

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

    @abstractmethod
    def select(self):
        pass

    @abstractmethod
    def update(self, data: dict, where: str):
        pass

    @abstractmethod
    def delete(self, where: str):
        pass

    def close(self):
        """
        데이터베이스 연결을 닫습니다.
        """
        if self.db_engine:
            self.db_engine.close()
