import threading
from queue import Empty, Full
from time import time
from .container import Container
from .adapter.database import DatabaseWarning


class BigQueue(Container):
    HASH = "_hash"
    OBJECT = "_object"

    def __init__(self, adapter=None, name=None, maxsize=0):
        super().__init__(adapter, name)
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.not_full = threading.Condition(self.mutex)
        self.all_tasks_done = threading.Condition(self.mutex)
        self.unfinished_tasks = 0
        self.maxsize = maxsize
        self._init_database()

    def _init_database(self):
        try:
            self._adapter.create_table(self._table, {
                self.OBJECT: self._adapter.blob,
                self.HASH: "VARCHAR(50)"})
        except DatabaseWarning:
            pass

    def get(self, block=True, timeout=None):
        with self.not_empty:
            if not block:
                if not self.qsize():
                    raise Empty
            elif timeout is None:
                while not self.qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while not self.qsize():
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self._get()
            self.not_full.notify()
            return item

    def _get(self):
        result = self._adapter.find_one(self._table, {})
        self._adapter.remove(self._table, {self.HASH: result.get(self.HASH)})
        return self._loads(result.get(self.OBJECT))

    def put(self, obj, block=True, timeout=None):
        with self.not_full:
            if self.maxsize > 0:
                if not block:
                    if self.qsize() >= self.maxsize:
                        raise Full
                elif timeout is None:
                    while self.qsize() >= self.maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = time() + timeout
                    while self.qsize() >= self.maxsize:
                        remaining = endtime - time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
            self._put(obj)
            self.unfinished_tasks += 1
            self.not_empty.notify()

    def _put(self, obj):
        binary_obj = self._dumps(obj)
        data = {self.OBJECT: binary_obj,
                self.HASH:   str(hash(binary_obj))}
        if hasattr(self._adapter, "add_one_binary"):
            self._adapter.add_one_binary(self._table, data)
        else:
            self._adapter.add_one(self._table, data)

    def qsize(self):
        return self.__len__()

    def empty(self):
        with self.mutex:
            return not self.qsize()

    def full(self):
        with self.mutex:
            return 0 < self.maxsize <= self.qsize()

    def task_done(self):
        with self.all_tasks_done:
            unfinished = self.unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    raise ValueError('task_done() called too many times')
                self.all_tasks_done.notify_all()
            self.unfinished_tasks = unfinished

    def join(self, timeout=None):
        with self.all_tasks_done:
            if timeout is None:
                while self.unfinished_tasks:
                    self.all_tasks_done.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while self.unfinished_tasks:
                    remaining = endtime - time()
                    if remaining < 0.0:
                        raise TimeoutError("wait task done timeout.")
                    self.all_tasks_done.wait(remaining)

    def put_nowait(self, obj):
        self.put(obj, block=False)

    def get_nowait(self):
        return self.get(block=False)


class BigPriorityQueue(BigQueue):
    PRIORITY = "_priority"

    def _put(self, obj):
        if not hasattr(obj, "priority"):
            raise TypeError("no priority attribute.")

        binary_obj = self._dumps(obj)
        if callable(obj.priority):
            priority = obj.priority()
        else:
            priority = obj.priority
        data = {self.OBJECT: binary_obj,
                self.PRIORITY: priority,
                self.HASH: str(hash(binary_obj))}
        if hasattr(self._adapter, "add_one_binary"):
            self._adapter.add_one_binary(self._table, data)
        else:
            self._adapter.add_one(self._table, data)

    def _get(self):
        result = self._adapter.find_one(self._table, {},
                                        orderby=self.PRIORITY,
                                        asc=True)
        self._adapter.remove(self._table, {self.HASH: result.get(self.HASH)})
        return self._loads(result.get(self.OBJECT))

    def _init_database(self):
        try:
            self._adapter.create_table(self._table, {
                self.OBJECT: self._adapter.blob,
                self.PRIORITY: "INT",
                self.HASH: "VARCHAR(50)"})
        except DatabaseWarning:
            pass
