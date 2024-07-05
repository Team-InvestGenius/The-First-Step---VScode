
class DataBase:

    def __init__(
            self,
            db_address,
            user_id,
            pw,

    ):

        self.db_address = db_address
        self.user_id = user_id
        self.pw = pw
        self.db_engine = None # DB Connector 가 반환될 것임

    def select(self, table: str):
        sql = f"SELECT * FROM {table} limit 10;"
        table = self.db_engine.sql(sql)
        return table

    def insert(self, table: str):
        pass

    def update(self):
        pass

    def delete(self):
        pass


