import pymysql
from .database import ABCDatabase, DatabaseError


class MySQLDB(ABCDatabase):

    def __init__(self):
        super().__init__()

    def _reconnect(self):
        self._conn = pymysql.connect(
            host = self._host,
            port = self._port,
            user = self._user,
            password = self._passwd,
            charset = self._charset,
            cursorclass = self._cursorclass or pymysql.cursors.DictCursor,
            db = self._db)

        if not self._conn.open:
            raise DatabaseError(f"Connect to {user}@{host}:{port} failed.")

    def close(self):
        self.cursor.close()
        self.conn.close()

    @property
    def is_open(self):
        return self._conn.open

    def _table_exist_sql(self, table):
        return f"SELECT COUNT(*) FROM information_schema.tables WHERE \
            table_name='{table}'"
            