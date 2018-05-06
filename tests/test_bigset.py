import unittest
import random
from skua.adapter.mysql import MySQLDB
from skua.adapter.mongo import MongoDB
from skua.bigset import BigSet

TEST_DB = "skua_test"
_STR = "asbcdefhijklmnopqrstuvwxyz_"

def random_str(k):
    return "".join(random.choices(_STR, k=5))


class TestBigSetSQLite(unittest.TestCase):
    def get_bs(self):
        return BigSet()

    def test_add(self):
        bs = self.get_bs()
        count = random.randint(30, 50)
        for _ in range(count):
            string = random_str(random.randint(1, 9999))
            bs.add(string)
            bs.add(string)
        self.assertEqual(len(bs), count)
        bs.delete()

    def test_len(self):
        bs = self.get_bs()
        count = random.randint(30, 50)
        for _ in range(count):
            string = random_str(random.randint(1, 9999))
            bs.add(string)

        self.assertEqual(len(bs), count)
        bs.delete()

    def test_update(self):
        bs = self.get_bs()
        count = random.randint(30, 50)
        lis = []
        for _ in range(count):
            string = random_str(random.randint(1, 9999))
            lis.append(string)

        bs.update(lis)
        self.assertEqual(len(bs), count)
        bs.delete()

    def test_remove(self):
        bs = self.get_bs()
        count = random.randint(30, 50)
        lis = []
        for _ in range(count):
            string = random_str(random.randint(1, 9999))
            lis.append(string)

        bs.update(lis)

        for string in lis:
            self.assertTrue(string in bs)
            bs.remove(string)
            self.assertFalse(string in bs)

        bs.delete()
        
    def test_iter(self):
        bs = self.get_bs()
        count = random.randint(30, 50)
        lis = []

        for _ in range(count):
            string = random_str(random.randint(1, 9999))
            bs.add(string)
            lis.append(string)
        
        for string in bs:
            self.assertTrue(string in lis)
        
        bs.delete()

    def test_pop(self):
        bs = self.get_bs()
        count = random.randint(30, 50)
        strings = []
        for _ in range(count):
            string = random_str(random.randint(1, 9999))
            strings.append(string)
            bs.add(string)
            bs.add(string)
        self.assertEqual(len(bs), count)

        for _ in range(count):
            string =  bs.pop()
            self.assertTrue(string in strings)

        self.assertEqual(len(bs), 0)
        bs.delete()

    def test_type_checker(self):
        bs = self.get_bs()
        count = random.randint(30, 50)

        class Test:
            def __hash__(self):
                return None

        class TestNoHash:
            pass

        for _ in range(count):
            with self.assertRaises(TypeError):
                bs.add(TestNoHash())
            with self.assertRaises(TypeError):
                bs.add(Test())

        with self.assertRaises(TypeError):
            bs.update(random.randint(0, 100))

        with self.assertRaises(KeyError):
            bs.pop()

        self.assertEqual(len(bs), 0)
        bs.delete()


class TestBigSetMySQL(TestBigSetSQLite):
    def get_bd(self):
        mysql = MySQLDB()
        mysql.connect(passwd="")
        mysql.select_db(TEST_DB)
        return BigSet(mysql)


class TestBigSetMongo(TestBigSetSQLite):
    def get_bd(self):
        mongo = MongoDB()
        mongo.connect(db=TEST_DB)
        return BigSet(mongo)
