import unittest
import random
from queue import Full, Empty
from skua.bigqueue import BigQueue
from skua.adapter.mysql import MySQLDB
from skua.adapter.mongo import MongoDB

TEST_DB = "skua_test"
_STR = "asbcdefhijklmnopqrstuvwxyz_"

def random_str(k):
    return "".join(random.choices(_STR, k=5))


class TestBigQueueSQLite(unittest.TestCase):
    def get_queue(self, maxsize=0):
        return BigQueue(maxsize=maxsize)

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
        q.delete()

    def test_qsize(self):
        q = self.get_queue()
        count = random.randint(30, 50)
        for k in range(count):
            string = random_str(k)
            q.put(string)
        
        self.assertEqual(q.qsize(), count)
        q.delete()

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
        
        with self.assertRaises(TimeoutError):
            q.get(timeout=1)

        q.delete()

    def test_put_timeout(self):
        count = random.randint(30, 50)
        q = self.get_queue(count)
        for k in range(count):
            string = random_str(k)
            q.put(string)
        
        with self.assertRaises(TimeoutError):
            string = random_str(k)
            q.put(string, timeout=1)

        q.delete()

    def test_put_nowait(self):
        count = random.randint(30, 50)
        q = self.get_queue(count)
        for k in range(count):
            string = random_str(k)
            q.put(string)
        
        with self.assertRaises(Full):
            string = random_str(k)
            q.put_nowait(string)

        q.delete()

    def test_get_nowait(self):
        count = random.randint(30, 50)
        q = self.get_queue()
        for k in range(count):
            string = random_str(k)
            q.put(string)

        for _ in range(count):
            self.assertTrue(q.get_nowait() in lst)

        with self.assertRaises(Empty):
            q.get_nowait()

        q.delete()

    def test_join(self):
        count = random.randint(30, 50)
        q = self.get_queue()
        for k in range(count):
            string = random_str(k)
            q.put(string)

        for _ in range(count):
            self.assertTrue(q.get_nowait() in lst)
            q.task_done()

        q.join()
        self.assertTrue(True)

        q.delete()

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

        q.delete()


class TestBigQueueMySQL(TestBigQueueSQLite):
    def get_queue(self, maxsize=0):
        mysql = MySQLDB()
        mysql.connect(passwd="")
        mysql.select_db(TEST_DB)
        return BigQueue(mysql, maxsize=maxsize)


class TestBigQueueMongo(TestBigQueueSQLite):
    def get_queue(self, maxsize=0):
        mongo = MongoDB()
        mongo.connect(db=TEST_DB)
        return BigQueue(mongo, maxsize=maxsize)
