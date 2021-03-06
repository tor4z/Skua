import unittest
import random
from skua.adapter.sqlite import SQLiteDB
from skua.adapter.database import (DatabaseError,
                                   DatabaseWarning)

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

    def test_add_many(self):
        sqlite = self.new_db()
        table = "test_add_many"
        count = 50
        sqlite.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            users.append({"name": name, 
                "age": age})

        sqlite.add_many(table, users)
        users = sqlite.find_many(table, {})
        self.assertEqual(len(users), count)

        sqlite.delete_table(table)
        sqlite.close()

    def test_count(self):
        sqlite = self.new_db()
        table = "test_count"
        count = 100
        sqlite.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        
        users = []
        eq5 = 0
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 10)
            if age == 5:
                eq5 += 1
            users.append({
                "name": name, 
                "age": age})
        sqlite.add_many(table, users)
        self.assertEqual(sqlite.count(table, {}), count)
        self.assertEqual(sqlite.count(table, {"age": 5}), eq5)

        sqlite.delete_table(table)
        sqlite.close()

    def test_add_update(self):
        sqlite = self.new_db()
        table = "test_add_update"
        count = 50
        sqlite.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        for _ in range(count):
            name = random_str(20)
            age = random.randint(0, 49)
            user = {"name": name, 
                    "age": age}
            sqlite.add_update(table, user)

        for _ in range(count):
            user = sqlite.find_one(table, {})
            user["age"] = random.randint(50, 100)
            sqlite.add_update(table, user, {"name": user["name"]})
            new_user = sqlite.find_one(table, {"name": user["name"]})
            self.assertTrue(user["age"] == new_user["age"])

            new_users = sqlite.find_many(table, {"name": user["name"]})
            self.assertEqual(len(new_users), 1)

            user["age"] = random.randint(101, 120)
            sqlite.add_update(table, user)
            new_users = sqlite.find_many(table, {"name": user["name"]})
            self.assertEqual(len(new_users), 2)
            sqlite.remove(table, {"name": user["name"]})

        sqlite.delete_table(table)
        sqlite.close()

    def test_find_gt_ge_lt_le(self):
        sqlite = self.new_db()
        table = "test_find_gt_ge_lt_le"
        count = 200
        sqlite.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = {"name": name, 
                    "age": age}
            users.append(user)
            sqlite.add_one(table, user)

        users_gt_50 = sqlite.find_many(table, {"age": SQLiteDB.gt(50)})
        for user in users_gt_50:
            self.assertGreater(user["age"], 50)

        users_ge_50 = sqlite.find_many(table, {"age": SQLiteDB.ge(50)})
        for user in users_ge_50:
            self.assertGreaterEqual(user["age"], 50)

        users_lt_50 = sqlite.find_many(table, {"age": SQLiteDB.lt(50)})
        for user in users_lt_50:
            self.assertLess(user["age"], 50)

        users_le_50 = sqlite.find_many(table, {"age": SQLiteDB.le(50)})
        for user in users_le_50:
            self.assertLessEqual(user["age"], 50)

        sqlite.delete_table(table)
        sqlite.close()

    def test_type_checker(self):
        sqlite = self.new_db()
        table = "test_find_one_order"
        count = 5
        sqlite.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = [("name", name),
                    ("age", age)]
            users.append(user)
            with self.assertRaises(TypeError):
                sqlite.add(table, user)
            
            with self.assertRaises(TypeError):
                sqlite.add_many(table, {})

    def test_connect_twice(self):
        sqlite = SQLiteDB()
        sqlite.connect()
        with self.assertRaises(DatabaseError):
            sqlite.connect()
        sqlite.close()

    def test_find_many_order(self):
        sqlite = self.new_db()
        table = "test_find_many_order"
        count = 50
        sqlite.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = {"name": name, 
                    "age": age}
            users.append(user)
            sqlite.add_one(table, user)

        users = sqlite.find_many(table, {}, orderby="age")

        old_age = 0
        for user in users:
            self.assertGreaterEqual(user["age"], old_age)
            old_age = user["age"]

        sqlite.delete_table(table)
        sqlite.close()
    
    def test_find_one_order(self):
        sqlite = self.new_db()
        table = "test_find_one_order"
        count = 50
        sqlite.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})

        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = {"name": name, 
                    "age": age}
            users.append(user)
            sqlite.add_one(table, user)

        old_age = 0
        for _ in range(count):
            user = sqlite.find_one(table, {}, orderby="age")
            self.assertGreaterEqual(user["age"], old_age)
            old_age = user["age"]

        sqlite.delete_table(table)
        sqlite.close()

