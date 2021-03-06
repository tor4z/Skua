import sqlite3
import threading
from .database import (ABCDatabase,
                       DatabaseError,
                       DatabaseWarning)


def results_gen(results):
    for result in results:
        yield dict(result)


class SQLiteDB(ABCDatabase):
    blob = "BLOB"
    ID = "id"

    def __init__(self):
        super().__init__()
        self.mutex = threading.Lock()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def connect(self, database=None, timeout=5):
        if self._connected:
            raise DatabaseError("Can not connect twice to a database \
                                in a instance.")
        self._database = database or ":memory:"
        self._timeout = timeout
        self._connected = True

    def _reconnect(self):
        # Using SQLite In Multi-Threaded Applications
        # Serialized. In serialized mode, SQLite can be safely used by
        # multiple threads with no restriction.
        # http://www.sqlite.org/threadsafe.html

        # By default, check_same_thread is True and only the creating
        # thread may use the connection. If set False, the returned
        # connection may be shared across multiple threads. When using
        # multiple threads with the same connection writing operations
        # should be serialized by the user to avoid data corruption.
        # https://docs.python.org/3/library/sqlite3.html#sqlite3.connect

        self._conn = sqlite3.connect(
            database=self._database,
            timeout=self._timeout,
            check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

    @property
    def is_open(self):
        return True

    def _table_to_sql(self, table, fields):
        field_str = ""
        for key, value in fields.items():
            field_str += f"{key} {value},"
        field_str = field_str[:-1]
        return f"CREATE  TABLE {table} ({field_str})"

    def select_db(self, *args, **kwarsg):
        raise DatabaseWarning(f"Command 'select db' not suport \
                              in {self.__class__.__name__}.")

    def create_db(self, *args, **kwargs):
        raise DatabaseWarning(f"Command 'create db' not suport \
                              in {self.__class__.__name__}.")

    def _table_exist_sql(self, table):
        return f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' \
               AND name='{table}'"

    def _list_to_insert_many_sql(self, table, fields):
        if not isinstance(fields, list):
            raise TypeError("List required.")

        key_str = ""
        value_str = ""
        for key in fields[0].keys():
            key_str += f"{key},"
            value_str += f":{key},"

        return f"INSERT INTO {table} ({key_str[:-1]}) VALUES \
               ({value_str[:-1]})"

    def _dict_to_insert_sql(self, table, fields):
        if not isinstance(fields, dict):
            raise TypeError("Dict required.")

        key_str = ""
        value_str = ""
        for key in fields.keys():
            key_str += f"{key},"
            value_str += f":{key},"

        return f"INSERT INTO {table} ({key_str[:-1]}) VALUES \
               ({value_str[:-1]})"

    def execute(self, sql, args={}):
        with self.mutex:
            rows = self.cursor.execute(sql, args)
            self.conn.commit()
            return rows

    def executemany(self, sql, args):
        with self.mutex:
            rows = self.cursor.executemany(sql, args)
            self.conn.commit()
            return rows

    def _find(self, table, fields, orderby=None, asc=True,
              limit=None, offset=0):
        sql = self._dict_to_find_sql(table, fields, orderby, asc,
                                     limit, offset)
        self.execute(sql)
        if limit == 1:
            result = self.cursor.fetchone()
            return dict(result) if result else None
        else:
            results = self.cursor.fetchall()
            return list(results_gen(results)) if results else []

    def add_one(self, table, fields):
        sql = self._dict_to_insert_sql(table, fields)
        return self.execute(sql, fields)
