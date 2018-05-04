from .container import Container
from .adapter.database import DatabaseWarning
import threading


class BigQueue(Container):
    INDEX = "_index"
    OBJECT = "_object"

    def __init__(self, adapter=None, name=None):
        super().__init__(adapter, name)
        try:
            self._adapter.create_table(self._table, {
                self.OBJECT: self._adapter.blob})
        except DatabaseWarning:
            pass

        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.not_full = threading.Condition(self.mutex)
        self.all_tasks_done = threading.Condition(self.mutex)
        self.unfinished_tasks = 0

    def get(self):
        pass

    def put(self):
        pass

    def qsize(self):
        pass

    def empty(self):
        pass

    def full(self):
        pass

    def task_done(self):
        pass

    def join(self):
        pass

    def put_nowait(self):
        pass

    def get_nowait(self):
        pass
