import unittest
import random
from queue import Full, Empty
from skua.bigqueue import BigQueue, BigPriorityQueue
from skua.adapter.mysql import MySQLDB
from skua.adapter.mongo import MongoDB

TEST_DB = "skua_test"
_STR = "asbcdefhijklmnopqrstuvwxyz_"

def random_str(k):
    return "".join(random.choices(_STR, k=5))

class PriorityV:

    def __init__(self, data):
        self.data = data
        self.priority = random.randint(0, 20)


class PriorityF:

    def __init__(self, data):
        self.data = data

    def priority(self):
        return random.randint(0, 20)


class TestBigQueueSQLite(unittest.TestCase):
    def get_queue(self, maxsize=0):
        q = BigQueue(maxsize=maxsize)
        q.clear()
        return q

    def test_put_get(self):
        q = self.get_queue()
        count = random.randint(30, 50)
        lst = []
        for k in range(count):
            string = random_str(k)
            lst.append(string)
            q.put(string)
        
        for _ in range(count):
            self.assertTrue(q.get(block=False) in lst)

    def test_qsize(self):
        q = self.get_queue()
        count = random.randint(30, 50)
        for k in range(count):
            string = random_str(k)
            q.put(string)
        
        self.assertEqual(q.qsize(), count)

    def test_get_timeout(self):
        q = self.get_queue()
        count = random.randint(30, 50)
        lst = []
        for k in range(count):
            string = random_str(k)
            lst.append(string)
            q.put(string)

        for _ in range(count):
            self.assertTrue(q.get(block=False) in lst)
        
        with self.assertRaises(Empty):
            q.get(timeout=1)

    def test_put_timeout(self):
        count = random.randint(30, 50)
        q = self.get_queue(count)
        for k in range(count):
            string = random_str(random.randint(20, 300))
            q.put_nowait(string)
        
        with self.assertRaises(Full):
            string = random_str(k)
            q.put(string, timeout=1)

    def test_put_nowait(self):
        count = random.randint(30, 50)
        q = self.get_queue(count)
        for k in range(count):
            string = random_str(random.randint(10, 200))
            q.put_nowait(string)
        
        with self.assertRaises(Full):
            string = random_str(k)
            q.put_nowait(string)

    def test_get_nowait(self):
        count = random.randint(30, 50)
        q = self.get_queue()
        lst = []
        for k in range(count):
            string = random_str(k)
            lst.append(string)
            q.put(string)

        for _ in range(count):
            self.assertTrue(q.get_nowait() in lst)

        with self.assertRaises(Empty):
            q.get_nowait()

    def test_join(self):
        count = random.randint(30, 50)
        q = self.get_queue()
        lst = []
        for k in range(count):
            string = random_str(k)
            lst.append(string)
            q.put(string)

        for _ in range(count):
            self.assertTrue(q.get_nowait() in lst)
            q.task_done()

        q.join()
        self.assertTrue(True)

    def test_join_timeout(self):
        count = random.randint(30, 50)
        q = self.get_queue()
        lst = []
        for k in range(count):
            string = random_str(k)
            lst.append(string)
            q.put(string)

        for _ in range(count-1):
            self.assertTrue(q.get_nowait() in lst)
            q.task_done()

        with self.assertRaises(TimeoutError):
            q.join(timeout=1)

    def get_priority_queue(self, maxsize=0):
        q = BigPriorityQueue(maxsize=maxsize)
        q.clear()
        return q

    def test_priority_put_get_val(self):
        q = self.get_priority_queue()
        count = random.randint(30, 50)
        lst = []
        for k in range(count):
            obj = PriorityV(random_str(k))
            lst.append(obj)
            q.put(obj)
        
        old = None
        for _ in range(count):
            obj = q.get(block=False)
            if old:
                self.assertLessEqual(old.priority, obj.priority)
            old = obj

    def test_priority_put_get_func(self):
        q = self.get_priority_queue()
        count = random.randint(30, 50)
        lst = []
        for k in range(count):
            obj = PriorityF(random_str(k))
            lst.append(obj)
            q.put(obj)
        
        old = None
        for _ in range(count):
            obj = q.get(block=False)
            if old:
                self.assertLessEqual(old.priority(), obj.priority())
            old = obj


class TestBigQueueMySQL(TestBigQueueSQLite):
    def get_queue(self, maxsize=0):
        mysql = MySQLDB()
        mysql.connect(passwd="")
        mysql.select_db(TEST_DB)
        q = BigQueue(mysql, maxsize=maxsize)
        q.clear()
        return q

    def get_priority_queue(self, maxsize=0):
        mysql = MySQLDB()
        mysql.connect(passwd="")
        mysql.select_db(TEST_DB)
        q = BigPriorityQueue(mysql, maxsize=maxsize)
        q.clear()
        return q


class TestBigQueueMongo(TestBigQueueSQLite):
    def get_queue(self, maxsize=0):
        mongo = MongoDB()
        mongo.connect(db=TEST_DB)
        q = BigQueue(mongo, maxsize=maxsize)
        q.clear()
        return q

    def get_priority_queue(self, maxsize=0):
        mongo = MongoDB()
        mongo.connect(db=TEST_DB)
        q = BigPriorityQueue(mongo, maxsize=maxsize)
        q.clear()
        return q
