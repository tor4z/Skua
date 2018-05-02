import unittest
import random
from skua.adapter import MySQLDB
from skua.bigdict import BigDict

TEST_DB = "skua_test"
_STR = "asbcdefhijklmnopqrstuvwxyz_"

def random_str(k):
    return "".join(random.choices(_STR, k=5))

class TestBigDictSQLite(unittest.TestCase):
    def test_setitem_getitem(self):
        bd = BigDict()
        for _ in range(50):
            # test string
            key = random_str(10)
            value = random_str(20)
            bd[key] = value
            self.assertEqual(bd[key], value)

            # test int
            key = random_str(10)
            value = random.randint(0, 999999)
            bd[key] = value
            self.assertEqual(bd[key], value)

    def test_delitem(self):
        pass

    def test_len(self):
        pass

class TestBigDictMySQL(unittest.TestCase):
    def test_setitem_getitem(self):
        mysql = MySQLDB()
        mysql.connect(passwd="")
        mysql.create_db(TEST_DB)
        mysql.select_db(TEST_DB)
        bd = BigDict(mysql)

        for _ in range(50):
            # test string
            key = random_str(10)
            value = random_str(20)
            bd[key] = value
            self.assertEqual(bd[key], value)

            # test int
            key = random_str(10)
            value = random.randint(0, 999999)
            bd[key] = value
            self.assertEqual(bd[key], value)

    def test_delitem(self):
        pass

    def test_len(self):
        pass