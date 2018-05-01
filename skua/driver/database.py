class ABCDatabase:
    operators = [">", "<", ">=", "<=", "="]

    @classmethod
    def ensure_operator(cls, value):
        if isinstance(value, str):
            if value[:1] in cls.operators or\
                    value[:2] in cls.operators:
                    return value
        return cls.eq(value)

    @staticmethod
    def eq(value):
        return f"='{value}'"

    @staticmethod
    def gt(value):
        return f">'{value}'"

    @staticmethod
    def ge(value):
        return f">='{value}'"

    @staticmethod
    def lt(value):
        return f"<'{value}'"

    @staticmethod
    def le(value):
        return f"<='{value}'"

    def __init__(self):
        self._conn = None
        self._cursor = None
        self._connected = False

    def connect(self, host=None, port=None, user=None, passwd=None, db=None, 
            charset=None, cursorclass=None):
        if self._connected:
            raise DatabaseError("Can not connect twice to a database in a instance.")

        self._host = host or "localhost"
        self._port = port or 3306
        self._user = user or "root"
        self._passwd = passwd
        self._db = db
        self._charset = charset or "utf8mb4"
        self._cursorclass = cursorclass
        self._reconnect()
        self._connected = True

    def close(self):
        raise NotImplementedError
    
    def _reconnect(self):
        raise NotImplementedError

    def _dict_to_find_sql(self, table, fields, 
            orderby=None, asc=True):
        raise NotImplementedError

    def _dict_to_insert_sql(self, table, fields):
        raise NotImplementedError

    def _dict_to_update_sql(self, table, update, where):
        raise NotImplementedError

    def _dict_to_delete_sql(self, table, fields=None):
        raise NotImplementedError

    def _table_to_sql(self, table, fields):
        raise NotImplementedError

    def select_db(self, db):
        self.conn.select_db(db)

    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self.conn.cursor()
        return self._cursor

    @property
    def conn(self):
        if not self._connected:
            raise DatabaseError("Databse not connected.") 

        if self._conn is None or not self._conn.open:
            self._reconnect()
        return self._conn

    def execute(self, sql, kwargs=None):
        rows = self.cursor.execute(sql, kwargs)
        self.conn.commit()
        return rows
            
    def executemany(self, sql, args):
        rows = self.cursor.executemany(sql, args)
        self.conn.commit()
        return rows

    def create_table(self, table, fields):
        sql = self._table_to_sql(table, fields)
        self.execute(sql)

    def delete_table(self, table):
        sql = f"DROP TABLE `{table}`"
        return self.execute(sql)

    def table_exit(self, table):
        sql = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{table}'"
        self.execute(sql)
        result = self.cursor.fetchone()
        return result["COUNT(*)"] == 1

    def add_one(self, table, fields):
        sql = self._dict_to_insert_sql(table, fields)
        return self.execute(sql, fields)

    def add_many(self, table, fields):
        if not isinstance(fields, list):
            raise TypeError("List requied.")
        sql = self._dict_to_insert_sql(table, fields[0])
        return self.executemany(sql, fields)

    def _find(self, table, fields, orderby=None, asc=True, 
            size=None, all=False):
        sql = self._dict_to_find_sql(table, fields, orderby, asc)
        rows = self.execute(sql)
        if size is None:
            if all:
                return self.cursor.fetchall()
            else: 
                return self.cursor.fetchone()
        else:
            size = min(max(size, 0), rows)
            return self.cursor.fetchmany(size)

    def find_one(self, table, fields, orderby=None, asc=True):
        return self._find(table, fields, orderby, asc)

    def find_many(self, table, fields, orderby=None, asc=True, 
            size=None):
        if size is None:
            return self._find(table, fields, orderby, asc, size, all = True)
        else:
            return self._find(table, fields, orderby, asc, size)

    def update(self, table, update, where):
        sql = self._dict_to_update_sql(table, update, where)
        return self.execute(sql)

    def remove(self, table, fields=None):
        sql = self._dict_to_delete_sql(table, fields)
        return self.execute(sql, fields)

class DatabaseError(Exception):
    pass