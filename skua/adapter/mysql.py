import pymysql
from .database import ABCDatabase, DatabaseError


class MySQLDB(ABCDatabase):
    blob = "BLOB"

    def __init__(self):
        super().__init__()

    def connect(self, host=None, port=None, user=None, passwd=None, db=None,
                charset=None, cursorclass=None):
        if self._connected:
            raise DatabaseError("Can not connect twice to a \
                                database in a instance.")

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
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._passwd,
            charset=self._charset,
            cursorclass=self._cursorclass or pymysql.cursors.DictCursor,
            db=self._db)

        if not self._conn.open:
            raise DatabaseError(f"Connect to {self._user}@{self._host}:\
                                {self._port} failed.")

    def close(self):
        self.cursor.close()
        self.conn.close()

    @property
    def is_open(self):
        return self._conn.open

    def _table_exist_sql(self, table):
        return f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE \
            TABLE_NAME='{table}'"

    def _db_exist_sql(self, db):
        return f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE \
            SCHEMA_NAME='{db}'"

    def _dict_to_insert_binary_sql(self, table, fields):
        if not isinstance(fields, dict):
            raise TypeError("Dict required.")

        key_str = ""
        value_str = ""
        for key in fields.keys():
            key_str += f"{key},"
            value_str += f"_binary %({key})s,"

        return f"INSERT INTO {table} ({key_str[:-1]}) VALUES \
               ({value_str[:-1]})"

    def _list_to_insert_many_binary_sql(self, table, fields):
        if not isinstance(fields, list):
            raise TypeError("List required.")

        key_str = ""
        value_str = ""
        for key in fields[0].keys():
            key_str += f"{key},"
            value_str += f"_binary %({key})s,"

        return f"INSERT INTO {table} ({key_str[:-1]}) VALUES \
               ({value_str[:-1]})"

    def add_one_binary(self, table, fields):
        sql = self._dict_to_insert_binary_sql(table, fields)
        return self.execute(sql, fields)

    def add_many_binary(self, table, fields):
        if not isinstance(fields, list):
            raise TypeError("List requied.")
        sql = self._list_to_insert_many_binary_sql(table, fields)
        return self.executemany(sql, fields)
