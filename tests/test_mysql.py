import unittest
import random
import pickle
from skua.adapter.mysql import MySQLDB
from skua.adapter.database import (DatabaseError,
                                   DatabaseWarning)

TEST_DB = "skua_test"
_STR = "asbcdefhijklmnopqrstuvwxyz_"

def random_str(k):
    return "".join(random.choices(_STR, k=5))

class TestMySQL(unittest.TestCase):
    def new_db(self):
        mysql = MySQLDB()
        mysql.connect(passwd="")
        mysql.select_db(TEST_DB)
        return mysql

    def test_exit_table(self):
        mysql = self.new_db()
        table = "testtable"
        self.assertFalse(mysql.table_exit("some_table_unexist"))
        mysql.create_table(table, {"name": "varchar(20)", 
            "age": "int"})
        self.assertTrue(mysql.table_exit(table))
        mysql.delete_table(table)
        mysql.close()
        
    def test_new_delete_table(self):
        mysql = self.new_db()
        for _ in range(5):
            table = random_str(5)
            mysql.create_table(table, {
                "name": "varchar(255)",
                "age" : "int"})
            self.assertTrue(mysql.table_exit(table))
            mysql.delete_table(table)
            self.assertFalse(mysql.table_exit(table))
        mysql.close()

    def test_insert_find_remove(self):
        mysql = self.new_db()
        table = "test_insert"
        mysql.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        for _ in range(50):
            name = random_str(5)
            age = random.randint(0, 100)
            mysql.add_one(table, {
                "name": name,
                "age": age})
            user = mysql.find_one(table, {
                "name": name})
            self.assertEqual(user["name"], name)

            name = random_str(5)
            age = 20
            mysql.add_one(table, {
                "name": name,
                "age": age})
            users = mysql.find_many(table, {"age": age})
            self.assertGreater(len(users), 0)
            least = 10
            users = mysql.find_many(table, {"age": MySQLDB.gt(least)})
            for user in users:
                self.assertGreater(user["age"], least)

        mysql.delete_table(table)
        mysql.close()

    def test_delete_item(self):
        mysql = self.new_db()
        table = "test_delete_item"
        mysql.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})

        for _ in range(50):
            name = random_str(5)
            age = random.randint(0, 100)
            mysql.add_one(table, {
                "name": name,
                "age": age})
            user = mysql.find_one(table, {"name": name})
            self.assertIsNotNone(user)
            mysql.remove(table, {"name": name})
            user = mysql.find_one(table, {"name": name})
            self.assertIsNone(user)

        mysql.delete_table(table)
        mysql.close()

    def test_update_item(self):
        mysql = self.new_db()
        table = "test_update_item"
        mysql.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})

        for _ in range(50):
            name = random_str(5)
            age = random.randint(0, 100)
            mysql.add_one(table, {
                "name": name,
                "age": age})

            old_user = mysql.find_one(table, {"name": name})
            new_age = random.randint(0, 100)
            mysql.update(table, {"age": new_age}, {"name": name})
            new_user = mysql.find_one(table, {"name": name})
            self.assertEqual(new_user["age"], new_age)

        mysql.delete_table(table)
        mysql.close()

    def test_add_many(self):
        mysql = self.new_db()
        table = "test_add_many"
        count = 50
        mysql.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            users.append({"name": name, 
                "age": age})

        mysql.add_many(table, users)
        users = mysql.find_many(table, {})
        self.assertEqual(len(users), count)

        mysql.delete_table(table)
        mysql.close()

    def test_count(self):
        mysql = self.new_db()
        table = "test_count"
        count = 100
        mysql.create_table(table, {
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
        mysql.add_many(table, users)
        self.assertEqual(mysql.count(table, {}), count)
        self.assertEqual(mysql.count(table, {"age": 5}), eq5)

        mysql.delete_table(table)
        mysql.close()

    def test_connect_twice(self):
        mysql = MySQLDB()
        mysql.connect(passwd="")
        with self.assertRaises(DatabaseError):
            mysql.connect(passwd="")

    def test_add_many_binary(self):
        mysql = self.new_db()
        table = "test_add_many_binary"
        count = 50
        mysql.create_table(table, {
            "user": "varchar(255)",
            "bin" : MySQLDB.blob})
        users = []
        for _ in range(count):
            users.append({
                "user": random_str(random.randint(1, 250)),
                "bin": pickle.dumps(random_str(random.randint(1, 250)))})

        mysql.add_many_binary(table, users)    
        self.assertEqual(mysql.count(table, {}), count)

        mysql.delete_table(table)
        mysql.close()

    def test_add_update(self):
        mysql = self.new_db()
        table = "test_add_update"
        count = 50
        mysql.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        for _ in range(count):
            name = random_str(20)
            age = random.randint(0, 49)
            user = {"name": name, 
                    "age": age}
            mysql.add_update(table, user)

        for _ in range(count):
            user = mysql.find_one(table, {})
            user["age"] = random.randint(50, 99)
            mysql.add_update(table, user, {"name": user["name"]})
            new_user = mysql.find_one(table, {"name": user["name"]})
            self.assertTrue(user["age"] == new_user["age"])

            new_users = mysql.find_many(table, {"name": user["name"]})
            self.assertEqual(len(new_users), 1)

            user["age"] = random.randint(100, 120)
            mysql.add_update(table, user)
            new_users = mysql.find_many(table, {"name": user["name"]})
            self.assertEqual(len(new_users), 2)
            mysql.remove(table, {"name": user["name"]})

        mysql.delete_table(table)
        mysql.close()

    def test_find_gt_ge_lt_le(self):
        mysql = self.new_db()
        table = "test_find_gt_ge_lt_le"
        count = 200
        mysql.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = {"name": name, 
                    "age": age}
            users.append(user)
            mysql.add_one(table, user)

        users_gt_50 = mysql.find_many(table, {"age": MySQLDB.gt(50)})
        for user in users_gt_50:
            self.assertGreater(user["age"], 50)

        users_ge_50 = mysql.find_many(table, {"age": MySQLDB.ge(50)})
        for user in users_ge_50:
            self.assertGreaterEqual(user["age"], 50)

        users_lt_50 = mysql.find_many(table, {"age": MySQLDB.lt(50)})
        for user in users_lt_50:
            self.assertLess(user["age"], 50)

        users_le_50 = mysql.find_many(table, {"age": MySQLDB.le(50)})
        for user in users_le_50:
            self.assertLessEqual(user["age"], 50)

        mysql.delete_table(table)
        mysql.close()

    def test_type_checker(self):
        mysql = self.new_db()
        table = "test_find_one_order"
        count = 5
        mysql.create_table(table, {
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
                mysql.add(table, user)
            
            with self.assertRaises(TypeError):
                mysql.add_many(table, {})

    def test_connect_twice(self):
        mysql = MySQLDB()
        mysql.connect()
        with self.assertRaises(DatabaseError):
            mysql.connect()
        mysql.close()

    def test_find_many_order(self):
        mysql = self.new_db()
        table = "test_find_many_order"
        count = 50
        mysql.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = {"name": name, 
                    "age": age}
            users.append(user)
            mysql.add_one(table, user)

        users = mysql.find_many(table, {}, orderby="age")

        old_age = 0
        for user in users:
            self.assertGreaterEqual(user["age"], old_age)
            old_age = user["age"]

        mysql.delete_table(table)
        mysql.close()
    
    def test_find_one_order(self):
        mysql = self.new_db()
        table = "test_find_one_order"
        count = 50
        mysql.create_table(table, {
            "name": "varchar(255)",
            "age" : "int"})

        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = {"name": name, 
                    "age": age}
            users.append(user)
            mysql.add_one(table, user)

        old_age = 0
        for _ in range(count):
            user = mysql.find_one(table, {}, orderby="age")
            self.assertGreaterEqual(user["age"], old_age)
            old_age = user["age"]

        mysql.delete_table(table)
        mysql.close()

