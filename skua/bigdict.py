import sqlite3
import pickle
from .adapter import ABCDatabase, SQLiteDB, DatabaseWarning

class BigDict:
    KEY = "_key"
    VALUE = "_value"

    def __init__(self, adapter=None, table=None):
        if adapter and not isinstance(adapter, ABCDatabase):
            raise TypeError("adapter should be a database object.")
        elif adapter:
            if not adapter.is_open:
                raise RuntimeError("adapter not connected.")

        if adapter:
            self._adapter = adapter
        else:
            self._adapter = SQLiteDB()
            self._adapter.connect()

        self._table = table or f"skua_{self.__class__.__name__}"
        if not self._adapter.table_exit(self._table):
            try:
                self._adapter.create_table(self._table, {
                    self.KEY: "VARCHAR(128)",
                    self.VALUE: self._adapter.blob})
            except DatabaseWarning:
                pass
    
    def __getitem__(self, key):
        result = self._adapter.find_one(self._table, {self.KEY: key})
        if result:
            value = result.get(self.VALUE)
            return pickle.loads(value)
        raise KeyError(key)

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            key = str(key)
        data = {self.KEY: key,
                self.VALUE: pickle.dumps(value, 2)}
        if hasattr(self._adapter, "add_one_binary"):
            self._adapter.add_one_binary(self._table, data)
        else:
            self._adapter.add_one(self._table, data)

    def __delitem__(self, key):
        self._adapter.remove(self._table, {self.KEY: key})

    def __len__(self):
        return self._adapter.count(self._table, {self.KEY: key})
