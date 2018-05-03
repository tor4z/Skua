from .adapter import (ABCDatabase, SQLiteDB, \
    DatabaseWarning)
from .container import Container

class BigDict(Container):
    KEY = "_key"
    VALUE = "_value"

    def __init__(self, adapter=None, name=None):
        super().__init__(adapter, name)
        if not self._adapter.table_exit(self._table):
            try:
                self._adapter.create_table(self._table, {
                    self.KEY: "VARCHAR(128)",
                    self.VALUE: self._adapter.blob})
            except DatabaseWarning:
                pass

    def _find_one_by_key(self, key):
        return self._adapter.find_one(self._table, {self.KEY: key})

    def __getitem__(self, key):
        result = self._find_one_by_key(key)
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

    def __contains__(self, key):
        result = result = self._find_one_by_key(key)
        if result:
            return True
        else:
            return False

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

