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
            cursorclass = self._cursorclass,
            db = self._db)

        if not self._conn.open:
            raise DatabaseError(f"Connect to {user}@{host}:{port} failed.")

    def close():
        self.corsor.close()
        self.conn.close()

    def _dict_to_find_sql(self, table, fields, orderby=None, asc=True):
        if not isinstance(fields, dict):
            raise TypeError("Dict required.")
        if orderby and not isinstance(orderby, list):
            orderby = [orderby]

        sql = f"SELECT * FROM `{table}` WHERE "
        order_str = ""

        for key in fields.keys():
            sql += f"`{key}`=%({key})s,"

        if orderby:
            for name in orderby:
                order_str += f"`{name}`,"
            order_str = order_str[:-1]
            order_str += "ASC" if asc else "DESC"

        return sql[:-1] + order_str

    def _dict_to_insert_sql(self, table, fields):
        if not isinstance(fields, dict):
            raise TypeError("Dict required.")

        key_str = ""
        value_str = ""
        for key in fields.keys():
            key_str += f"`{key}`,"
            value_str += f"%({key})s,"

        return f"INSERT INTO {table} ({key_str[:-1]}) VALUES ({value_str[:-1]})"

    def _dict_to_update_sql(self, table, update, where):
        if not isinstance(update, dict) or not isinstance(where, dict):
            raise TypeError("Dict required.")

        update_str = ""
        where_str = ""
        for key, value in update.items():
            update_str += f"`{key}`=`{value}`,"

        for key, value in where.items():
            where_str += f"`{key}`=`{value}`,"

        return f"UPDATE {table} SET {update_str[:-1]} WHERE {where_str[:-1]}"

    def _dict_to_delete_sql(self, table, fields=None):
        if fields is None:
            return f"DELETE * FROM `{table}`"
        
        sql = f"DELETE FROM `{table}` WHERE "
        for key in fields.keys():
            sql += f"`{key}`=%({key})s,"
        return sql[:-1]
        
    def _table_to_sql(self, table, fields):
        sql = f"CREATE  TABLE `{table}` "
        for key in args.keys():
            sql += f"`{key}` %({key})s`,"
        return sql[:-1]

