class ABCDatabase:
    operators = [">", "<", ">=", "<=", "="]
    blob = "BLOB"

    @classmethod
    def string_ensure(cls, value):
        if isinstance(value, str):
            return f'\"{value}\"'
        else:
            return f'{value}'

    @classmethod
    def ensure_operator(cls, value):
        if isinstance(value, str):
            if value[:1] in cls.operators or\
                    value[:2] in cls.operators:
                    return value
        return cls.eq(value)

    @classmethod
    def eq(cls, value):
        return "=" + cls.string_ensure(value)

    @classmethod
    def gt(cls, value):
        return f">{value}"

    @classmethod
    def ge(cls, value):
        return f">={value}"

    @classmethod
    def lt(cls, value):
        return f"<{value}"

    @classmethod
    def le(cls, value):
        return f"<={value}"

    def __init__(self):
        self._conn = None
        self._cursor = None
        self._connected = False

    def connect(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def _reconnect(self):
        raise NotImplementedError

    def _dict_to_find_sql(self, table, fields, orderby=None, asc=True,
                          limit=None, offset=0):
        if not isinstance(fields, dict):
            raise TypeError("Dict required.")
        if orderby and not isinstance(orderby, list):
            orderby = [orderby]
        if not isinstance(offset, int):
            raise TypeError("Offset should be a number.")

        sql = f"SELECT * FROM {table} "
        order_str = ""
        fields_str = "WHERE "
        offset_str = "" if limit is None else f" LIMIT {limit} OFFSET {offset}"

        for key, value in fields.items():
            fields_str += f" {key} {ABCDatabase.ensure_operator(value)} AND"
        fields_str = fields_str[:-3] if fields else ""

        if orderby:
            for name in orderby:
                order_str += f" {name},"
            order_str = order_str[:-1]
            order_str += "ASC" if asc else "DESC"

        return sql + fields_str + order_str + offset_str

    def _dict_to_insert_sql(self, table, fields):
        if not isinstance(fields, dict):
            raise TypeError("Dict required.")

        key_str = ""
        value_str = ""
        for key in fields.keys():
            key_str += f" {key},"
            value_str += f" %({key})s,"

        return f"INSERT INTO {table} ({key_str[:-1]}) VALUES \
            ({value_str[:-1]})"

    def _list_to_insert_many_sql(self, table, fields):
        if not isinstance(fields, list):
            raise TypeError("List required.")

        key_str = ""
        value_str = ""
        for key in fields[0].keys():
            key_str += f" {key},"
            value_str += f" %({key})s,"

        return f"INSERT INTO {table} ({key_str[:-1]}) VALUES \
            ({value_str[:-1]})"

    def _dict_to_update_sql(self, table, update, where):
        if not isinstance(update, dict) or not isinstance(where, dict):
            raise TypeError("Dict required.")

        update_str = ""
        where_str = ""
        for key, value in update.items():
            update_str += f"{key}='{value}',"

        for key, value in where.items():
            where_str += f" {key} {ABCDatabase.ensure_operator(value)} AND"

        return f"UPDATE {table} SET {update_str[:-1]} WHERE {where_str[:-3]}"

    def _dict_to_delete_sql(self, table, fields=None):
        if not fields:
            return f"DELETE FROM {table}"

        sql = f"DELETE FROM {table} WHERE "
        for key, value in fields.items():
            sql += f" {key} {ABCDatabase.ensure_operator(value)} AND"
        return sql[:-3]

    def _table_to_sql(self, table, fields):
        field_str = "id MEDIUMINT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
        for key, value in fields.items():
            field_str += f"{key} {value},"
        field_str = field_str[:-1]
        return f"CREATE  TABLE {table} ({field_str})"

    def _table_exist_sql(self, table):
        raise NotImplementedError

    def _db_exist_sql(self, db):
        raise NotImplementedError

    def _create_db_sql(self, db):
        return f"CREATE DATABASE IF NOT EXISTS {db}"

    def _delete_table_sql(self, table):
        return f"DROP TABLE {table}"

    def _count_sql(self, table, fields):
        if not isinstance(fields, dict):
            raise TypeError("Dict required.")

        sql = f"SELECT COUNT(*) FROM {table} "
        if fields:
            fields_str = "WHERE "
            for key, value in fields.items():
                fields_str += f" {key} {self.ensure_operator(value)} AND"
            fields_str = fields_str[:-3]
        else:
            fields_str = ""
        return sql + fields_str

    @property
    def is_open(self):
        raise NotImplementedError

    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self.conn.cursor()
        return self._cursor

    @property
    def conn(self):
        if not self._connected:
            raise DatabaseError("Databse not connected.")

        if self._conn is None or not self.is_open:
            self._reconnect()
        return self._conn

    def execute(self, sql, args={}):
        rows = self.cursor.execute(sql, args)
        self.conn.commit()
        return rows

    def executemany(self, sql, args):
        rows = self.cursor.executemany(sql, args)
        self.conn.commit()
        return rows

    def db_exist(self, db):
        sql = self._db_exist_sql(db)
        self.execute(sql)
        result = self.cursor.fetchone()
        return result["COUNT(*)"] == 1

    def create_table(self, table, fields):
        if self.table_exit(table):
            raise DatabaseWarning(f"Table {table} already exists.")
        sql = self._table_to_sql(table, fields)
        self.execute(sql)

    def delete_table(self, table):
        sql = self._delete_table_sql(table)
        return self.execute(sql)

    def table_exit(self, table):
        sql = self._table_exist_sql(table)
        self.execute(sql)
        result = self.cursor.fetchone()
        return result["COUNT(*)"] == 1

    def add_one(self, table, fields):
        sql = self._dict_to_insert_sql(table, fields)
        return self.execute(sql, fields)

    def add_many(self, table, fields):
        if not isinstance(fields, list):
            raise TypeError("List requied.")
        sql = self._list_to_insert_many_sql(table, fields)
        return self.executemany(sql, fields)

    def _find(self, table, fields, orderby=None, asc=True,
              limit=None, offset=0):
        sql = self._dict_to_find_sql(table, fields, orderby, asc,
                                     limit, offset)
        self.execute(sql)
        if limit == 1:
            return self.cursor.fetchone()
        else:
            return self.cursor.fetchall()

    def find_one(self, table, fields={}, orderby=None, asc=True,
                 offset=0):
        return self._find(table, fields, orderby, asc, limit=1,
                          offset=offset)

    def find_many(self, table, fields={}, orderby=None, asc=True,
                  limit=None, offset=0):
        return self._find(table, fields, orderby, asc,
                          limit=limit, offset=offset)

    def update(self, table, update, where):
        sql = self._dict_to_update_sql(table, update, where)
        return self.execute(sql)

    def remove(self, table, fields={}):
        sql = self._dict_to_delete_sql(table, fields)
        return self.execute(sql)

    def count(self, table, fields={}):
        sql = self._count_sql(table, fields)
        self.execute(sql)
        result = self.cursor.fetchone()
        return result["COUNT(*)"]

    def add_update(self, table, fields, where=None):
        where = where or fields
        result = self.find_one(table, where)
        if not result:
            if self.ID in fields:
                del fields[self.ID]
            self.add_one(table, fields)
        else:
            if result != fields:
                self.update(table, fields, where)


class DatabaseError(Exception):
    pass


class DatabaseWarning(Exception):
    pass
