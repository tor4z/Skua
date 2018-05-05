import pymongo
from urllib.parse import quote_plus
from .database import (ABCDatabase,
                       DatabaseError,
                       DatabaseWarning)


class MongoDB(ABCDatabase):
    ID = "_id"

    @staticmethod
    def gt(value):
        return {"$gt": value}

    @staticmethod
    def ge(value):
        return {"$gte": value}

    @staticmethod
    def lt(value):
        return {"$lt": value}

    @staticmethod
    def le(value):
        return {"$lte": value}

    def __init__(self):
        super().__init__()
        self._uri = None
        self._db = None
        self._database = None

    def connect(self, host=None, port=None, user=None, passwd=None, db=None):
        if self._connected:
            raise DatabaseError("Connect to database twice.")

        port = port or 27017
        host = host or "localhost"
        user = user or ""
        passwd = passwd or ""
        self._database = db
        if user and passwd:
            user_info = f"{quote_plus(user)}:{quote_plus(passwd)}@"
        else:
            user_info = ""

        self._uri = f"mongodb://{user_info}{quote_plus(host)}:{port}"
        self._reconnect()
        self._connected = True

    def _reconnect(self):
        self._conn = pymongo.MongoClient(self._uri)

    @property
    def is_open(self):
        if not self._conn:
            return False

        try:
            self._conn.is_mongos
            return True
        except pymongo.errors.ServerSelectionTimeoutError:
            return False

    @property
    def db(self):
        if self._db is None:
            self._db = self.conn.get_database(self._database)
        return self._db

    def close(self):
        self.conn.close()

    def select_db(self, db):
        self._db = self.conn.get_database(db)

    def create_db(self, *args, **kwargs):
        # PyMongo will create database aotomatically.
        raise DatabaseWarning("create_db NOT need in mongodb")

    def create_table(self, *args, **kwargs):
        # PyMongo will create table aotomatically.
        raise DatabaseWarning("create_table NOT need in mongodb")

    def delete_table(self, table):
        self.db.drop_collection(table)

    def table_exit(self, table):
        return table in self.db.collection_names(
                include_system_collections=False)

    def add_one(self, table, fields={}):
        if not isinstance(fields, dict):
            raise TypeError("Dict requied.")
        return self.db[table].insert_one(fields)

    def add_many(self, table, fields={}):
        if not isinstance(fields, list):
            raise TypeError("List requied.")
        return self.db[table].insert_many(fields)

    def find_one(self, table, fields={}, orderby=None, asc=True, offset=0):
        if not isinstance(fields, dict):
            raise TypeError("Dict requied.")
        if orderby:
            order = pymongo.ASCENDING if asc else pymongo.DESCENDING
            result = self.db[table].find_one(fields, sort=[(orderby, order)],
                                             skip=offset)
        else:
            result = self.db[table].find_one(fields, skip=offset)
        return result

    def find_many(self, table, fields={}, orderby=None, asc=True,
                  limit=0, offset=0):
        if orderby:
            order = pymongo.ASCENDING if asc else pymongo.DESCENDING
            result = self.db[table].find(fields).sort(orderby, order)\
                .skip(offset).limit(limit)
        else:
            result = self.db[table].find(fields).skip(offset).limit(limit)
        return list(result)

    def update(self, table, update={}, where={}):
        return self.db[table].update_many(where, {"$set": update})

    def remove(self, table, fields={}):
        return self.db[table].delete_many(fields)

    def count(self, table, fields):
        return self.db[table].find(fields).count()

    def add_update(self, table, fields, where=None):
        where = where or fields
        if self.ID in fields:
            del fields[self.ID]
        return self.db[table].update_one(where, {"$set": fields}, upsert=True)

    def add(self, table, fields={}):
        return self.add_one(table, fields)
