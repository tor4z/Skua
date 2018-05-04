from .container import Container
from .adapter.database import DatabaseWarning


class BigSet(Container):
    HASH = "_hash"
    OBJECT = "_object"

    def __init__(self, adapter=None, name=None):
        super().__init__(adapter, name)
        try:
            self._adapter.create_table(self._table, {
                self.HASH: "VARCHAR(50)",
                self.OBJECT: self._adapter.blob})
        except DatabaseWarning:
            pass

    def _get_hash(self, obj):
        if not hasattr(obj, "__hash__"):
            raise TypeError(f"unhashable type: {type(obj)}")
        hash = obj.__hash__()
        if hash is None:
            raise TypeError("__hash__ return None.")
        return str(hash)

    def _find_one_by_hash(self, hash):
        return self._adapter.find_one(self._table, {self.HASH: hash})

    def _remove_by_hash(self, hash):
        self._adapter.remove(self._table, {self.HASH: hash})

    def add(self, obj):
        hash = self._get_hash(obj)
        result = self._find_one_by_hash(hash)
        if not result:
            data = {self.HASH: hash,
                    self.OBJECT: self._dumps(obj)}

            if hasattr(self._adapter, "add_one_binary"):
                self._adapter.add_one_binary(self._table, data)
            else:
                self._adapter.add_one(self._table, data)

    def update(self, it):
        if not hasattr(it, "__iter__"):
            raise TypeError(f"{type(it)} is not iterable")
        for obj in it:
            self.add(obj)

    def remove(self, obj):
        hash = self._get_hash(obj)
        self._remove_by_hash(hash)

    def __iter__(self):
        offset = 0
        result = self._adapter.find_one(self._table, {}, offset=offset)
        while result:
            yield self._loads(result.get(self.OBJECT))
            offset += 1
            result = self._adapter.find_one(self._table, {}, offset=offset)

    def __contains__(self, obj):
        hash = self._get_hash(obj)
        result = self._find_one_by_hash(hash)
        if result:
            return True
        else:
            return False

    def pop(self):
        result = self._adapter.find_one(self._table, {})
        if result:
            self._remove_by_hash(result.get(self.HASH))
            return self._loads(result.get(self.OBJECT))
        else:
            raise KeyError("BigSet empty.")
