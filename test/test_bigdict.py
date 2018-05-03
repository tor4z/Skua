import unittest
import random
from skua.adapter import MySQLDB, MongoDB
from skua.bigdict import BigDict

TEST_DB = "skua_test"
_STR = "asbcdefhijklmnopqrstuvwxyz_"

def random_str(k):
    return "".join(random.choices(_STR, k=5))


class TestBigDictSQLite(unittest.TestCase):
    def get_bd(self):
        return BigDict()

    def test_setitem_getitem(self):
        bd = self.get_bd()
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

        bd.delete()

    def test_delitem(self):
        bd = self.get_bd()
        for _ in range(50):
            key = random_str(10)
            value = random_str(20)
            bd[key] = value

            self.assertEqual(bd[key], value)
            old_count = len(bd)
            del bd[key]
            self.assertIsNone(bd.get(key))
            self.assertEqual(len(bd), old_count-1)

        bd.delete()

    def test_len(self):
        bd = self.get_bd()
        length = random.randint(1, 50)
        for _ in range(length):
            key = random_str(10)
            value = random_str(20)
            bd[key] = value

        self.assertEqual(len(bd), length)
        bd.delete()

    def test_keys(self):
        bd = self.get_bd()
        length = random.randint(30, 50)
        keys = []
        for _ in range(length):
            key = random_str(10)
            keys.append(key)
            value = random_str(20)
            bd[key] = value

        n = 0
        for key in bd.keys():
            n += 1
            self.assertTrue(key in keys)
        self.assertEqual(n, length)
        bd.delete()

    def test_iter(self):
        # yield from keys
        bd = self.get_bd()
        length = random.randint(30, 50)
        keys = []
        for _ in range(length):
            key = random_str(10)
            keys.append(key)
            value = random_str(20)
            bd[key] = value

        n = 0
        for key in bd:
            n += 1
            self.assertTrue(key in keys)
        self.assertEqual(n, length)
        bd.delete()

    def test_values(self):
        # yield from keys
        bd = self.get_bd()
        length = random.randint(30, 50)
        values = []
        for _ in range(length):
            key = random_str(10)
            value = random_str(20)
            values.append(value)
            bd[key] = value

        n = 0
        for value in bd.values():
            n += 1
            self.assertTrue(value in values)
        self.assertEqual(n, length)
        bd.delete()

    def test_items(self):
        bd = self.get_bd()
        length = random.randint(30, 50)
        values = []
        keys = []
        dic = {}
        for _ in range(length):
            key = random_str(10)
            value = random_str(20)
            values.append(value)
            keys.append(key)
            bd[key] = value
            dic[key] = value

        n = 0
        for key, value in bd.items():
            n += 1
            self.assertTrue(value in values)
            self.assertTrue(key in keys)
            self.assertEqual(dic[key], value)

        self.assertEqual(n, length)
        bd.delete()

    def test_pop(self):
        bd = self.get_bd()
        dic = {}

        length = random.randint(30, 50)
        for _ in range(length):
            key = random_str(10)
            value = random_str(20)
            bd[key] = value
            dic[key] = value

        for key in dic.keys():
            self.assertEqual(dic[key], bd.pop(key))
        self.assertEqual(len(bd), 0)

        bd.delete()

    def test_popitem(self):
        bd = self.get_bd()
        dic = {}

        length = random.randint(30, 50)
        for _ in range(length):
            key = random_str(10)
            value = random_str(20)
            bd[key] = value
            dic[key] = value

        for _ in range(length):
            key, value = bd.popitem()
            self.assertEqual(dic[key], value)
        
        with self.assertRaises(KeyError):
            key, value = bd.popitem()

        bd.delete()

    def test_clear(self):
        bd = self.get_bd()

        length = random.randint(30, 50)
        for _ in range(length):
            key = random_str(10)
            value = random_str(20)
            bd[key] = value
        bd.clear()
        self.assertEqual(len(bd), 0)
        bd.delete()

    def test_update(self):
        bd = self.get_bd()
        dic = {}
        length = random.randint(30, 50)

        for _ in range(length):
            key = random_str(10)
            value = random_str(20)
            dic[key] = value
            
        bd.update(dic)
        self.assertEqual(len(bd), length)

        for key, value in dic.items():
            self.assertEqual(value, bd[key])

        bd.delete()


class TestBigDictMySQL(TestBigDictSQLite):
    def get_bd(self):
        mysql = MySQLDB()
        mysql.connect(passwd="")
        mysql.select_db(TEST_DB)
        return BigDict(mysql)


class TestBigDictMongo(TestBigDictSQLite):
    def get_bd(self):
        mongo = MongoDB()
        mongo.connect(db=TEST_DB)
        return BigDict(mongo)
