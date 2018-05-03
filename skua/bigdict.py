import pickle
from .adapter import ABCDatabase, SQLiteDB, DatabaseWarning

class BigDict:
    KEY = "_key"
    VALUE = "_value"

    @classmethod
    def _loads(cls, data):
        return pickle.loads(data)
    
    @classmethod
    def _dumps(csl, data):
        return pickle.dumps(data, 2)

    def __init__(self, adapter=None, name=None):
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

        self._table = name or f"skua_{self.__class__.__name__}"
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
            return self._loads(value)
        raise KeyError(key)

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            key = str(key)

        data = {self.KEY: key,
                self.VALUE: self._dumps(value)}
        if hasattr(self._adapter, "add_one_binary"):
            self._adapter.add_one_binary(self._table, data)
        else:
            self._adapter.add_one(self._table, data)

    def __delitem__(self, key):
        self._adapter.remove(self._table, {self.KEY: key})

    def __len__(self):
        return self._adapter.count(self._table, {})

    def __iter__(self):
        yield from self.keys()

    def keys(self):
        gen = self.items()
        while True:
            try:
                result = next(gen)
                yield result[0]
            except StopIteration:
                return

    def values(self):
        gen = self.items()
        while True:
            try:
                result = next(gen)
                yield result[1]
            except StopIteration:
                return

    def items(self):
        offset = 0
        result = self._adapter.find_one(self._table, {}, offset = offset)
        while result:
            yield (result[self.KEY], self._loads(result[self.VALUE]))
            offset += 1
            result = self._adapter.find_one(self._table, {}, offset = offset)

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def pop(self, key):
        result = self.__getitem__(key)
        self.__delitem__(key)
        return result

    def popitem(self):
        result = self._adapter.find_one(self._table, {})
        if result:
            key = result.get(self.KEY)
            value = result.get(self.VALUE)
            self.__delitem__(key)
            return (key, self._loads(value))
        else:
            raise KeyError("popitem(): BigDict is empty")

    def update(self, dic):
        if not isinstance(dic, dict):
            raise TypeError("Dict required.")
        for key, value in dic.items():
            if self.get(key) != value:
                self.__setitem__(key, value)

    def clear(self):
        self._adapter.remove(self._table, {})
    
    def __del__(self):
        self._adapter.close()

    def delete(self):
        self._adapter.delete_table(self._table)
