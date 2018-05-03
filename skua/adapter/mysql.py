import pymysql
from .database import ABCDatabase, DatabaseError


class MySQLDB(ABCDatabase):
    blob = "BLOB"

    def __init__(self):
        super().__init__()

    def connect(self, host=None, port=None, user=None, passwd=None, db=None, 
            charset=None, cursorclass=None):
        if self._connected:
            raise DatabaseError("Can not connect twice to a database in a instance.")

        self._host = host or "localhost"
        self._port = port or 3306
        self._user = user or "root"
        self._passwd = passwd or ""
        self._db = db
        self._charset = charset or "utf8"
        self._cursorclass = cursorclass
        self._reconnect()
        self._connected = True

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
            