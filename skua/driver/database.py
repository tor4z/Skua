class ABCDatabase:

    def __init__(self):
        self._conn = None
        self._cursor = None
        self._connected = False

    def connect(self, host, port, user, passwd, db, 
            charset=None, cursorclass=None):
        if self._connected:
            raise DatabaseError("Can not connect twice to a database in a instance.")

        self._host = host
        self._port = port
        self._user = user
        self._passwd = passwd
        self._db = db
        self._charset = charset or "utf8"
        self._cursorclass = cursorclass or pymysql.cursors.DictCursor
        self._reconnect()
        self._connected = True

    def close(self):
        raise NotImplementedError
    
    def _reconnect(self)
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

    @property
    def cursor(self):
        if self._cursor is not None:
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
        self.execute(sql, fields)

    def add_one(self, table, fields):
        sql = self._dict_to_insert_sql(table, fields)
        return self.execute(sql, fields)

    def add_many(self, table, fields):
        if not isinstance(fields, list):
            raise TypeError("List requied.")
        sql = self._dict_to_insert_sql(table, fields[0])
        return self.executemany(sql, fields)

    def _find(self, table, fields, orderby=None, asc=True, size=None):
        sql = self._dict_to_find_sql(table, fields, orderby, asc)
        rows = self.execute(sql, fields)
        if size is None:
            return self.cursor.fetchone()
        else:
            size = min(max(size, 0), rows)
            return self.cursor.fetchmany(size)

    def find_one(self, table, fields, orderby=None, asc=True):
        return self._find(table, fields, orderby, asc)

    def find_many(self, table, fields, orderby=None, asc=True, size=None):
        return self._find(table, fields, orderby, asc, size)

    def update(self, table, update, where):
        sql = self._dict_to_update_sql(table, update, where)
        return self.execute(sql)

    def remove(self, table, fields=None):
        sql = self._dict_to_delete_sql(table, fields)
        return self.execute(sql, fields)

class DatabaseError(Exception):
    pass