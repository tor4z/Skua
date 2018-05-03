import pickle
from .adapter import SQLiteDB, ABCDatabase

class Container:
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

    def clear(self):
        self._adapter.remove(self._table, {})
    
    def __del__(self):
        self._adapter.close()

    def __len__(self):
        return self._adapter.count(self._table, {})
        
    def delete(self):
        self._adapter.delete_table(self._table)
