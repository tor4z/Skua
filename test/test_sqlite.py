import unittest
import random
from skua.adapter.sqlite import SQLiteDB

TEST_DB = "skua_test"
_STR = "asbcdefhijklmnopqrstuvwxyz_"

def random_str(k):
    return "".join(random.choices(_STR, k=5))

class TestSQLite(unittest.TestCase):
    def new_db(self):
        sqlite = SQLiteDB()
        sqlite.connect()
        return sqlite

    def test_exit_table(self):
        sqlite = self.new_db()
        self.assertFalse(sqlite.table_exit("some_table_unexist"))
        sqlite.close()
        
    def test_new_delete_table(self):
        sqlite = self.new_db()
        for _ in range(5):
            table = random_str(5)
            sqlite.create_table(table, {
                "name": "varchar(255)",
                "age" : "int"})
            self.assertTrue(sqlite.table_exit(table))
            sqlite.delete_table(table)
            self.assertFalse(sqlite.table_exit(table))
        sqlite.close()

    def test_insert_find_remove(self):
        sqlite = self.new_db()
        table = "test_insert"
        sqlite.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        for _ in range(50):
            name = random_str(5)
            age = random.randint(0, 100)
            sqlite.add_one(table, {
                "name": name,
                "age": age})
            user = sqlite.find_one(table, {
                "name": name})
            self.assertEqual(user["name"], name)

            name = random_str(5)
            age = 20
            sqlite.add_one(table, {
                "name": name,
                "age": age})
            users = sqlite.find_many(table, {"age": age})
            self.assertGreater(len(users), 0)
            least = 10
            users = sqlite.find_many(table, {"age": SQLiteDB.gt(least)})
            for user in users:
                self.assertGreater(user["age"], least)

        sqlite.delete_table(table)
        sqlite.close()

    def test_delete_item(self):
        sqlite = self.new_db()
        table = "test_delete_item"
        sqlite.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})

        for _ in range(50):
            name = random_str(5)
            age = random.randint(0, 100)
            sqlite.add_one(table, {
                "name": name,
                "age": age})
            user = sqlite.find_one(table, {"name": name})
            self.assertIsNotNone(user)
            sqlite.remove(table, {"name": name})
            user = sqlite.find_one(table, {"name": name})
            self.assertIsNone(user)

        sqlite.delete_table(table)
        sqlite.close()

    def test_update_item(self):
        sqlite = self.new_db()
        table = "test_update_item"
        sqlite.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})

        for _ in range(50):
            name = random_str(5)
            age = random.randint(0, 100)
            sqlite.add_one(table, {
                "name": name,
                "age": age})

            old_user = sqlite.find_one(table, {"name": name})
            new_age = random.randint(0, 100)
            sqlite.update(table, {"age": new_age}, {"name": name})
            new_user = sqlite.find_one(table, {"name": name})
            self.assertEqual(new_user["age"], new_age)

        sqlite.delete_table(table)
        sqlite.close()