import unittest
import random
from skua.adapter.mongo import MongoDB
from skua.adapter.database import (DatabaseWarning,
                                   DatabaseError)

TEST_DB = "skua_test"
_STR = "asbcdefhijklmnopqrstuvwxyz_"

def random_str(k):
    return "".join(random.choices(_STR, k=5))

class TestMongoDB(unittest.TestCase):
    def new_db(self):
        mongo = MongoDB()
        mongo.connect(db=TEST_DB)
        return mongo

    def test_exit_table(self):
        mongo = self.new_db()
        self.assertFalse(mongo.table_exit("some_table_unexist"))
        mongo.close()
        
    def test_new_delete_table(self):
        mongo = self.new_db()
        for _ in range(5):
            table = random_str(5)
            with self.assertRaises(DatabaseWarning):
                mongo.create_table(table)
        mongo.close()

    def test_insert_find_remove(self):
        mongo = self.new_db()
        table = "test_insert"
        for _ in range(50):
            name = random_str(5)
            age = random.randint(0, 100)
            mongo.add_one(table, {
                "name": name,
                "age": age})
            user = mongo.find_one(table, {
                "name": name})
            self.assertEqual(user["name"], name)

            name = random_str(5)
            age = 20
            mongo.add_one(table, {
                "name": name,
                "age": age})
            users = mongo.find_many(table, {"age": age})
            self.assertGreater(len(users), 0)
            least = 10
            users = mongo.find_many(table, {"age": MongoDB.gt(least)})
            for user in users:
                self.assertGreater(user["age"], least)

        mongo.delete_table(table)
        mongo.close()

    def test_delete_item(self):
        mongo = self.new_db()
        table = "test_delete_item"

        for _ in range(50):
            name = random_str(5)
            age = random.randint(0, 100)
            mongo.add_one(table, {
                "name": name,
                "age": age})
            
            user = mongo.find_one(table, {"name": name})
            self.assertIsNotNone(user)
            mongo.remove(table, {"name": name})
            user = mongo.find_one(table, {"name": name})
            self.assertIsNone(user)

        mongo.delete_table(table)
        mongo.close()

    def test_update_item(self):
        mongo = self.new_db()
        table = "test_update_item"

        for _ in range(50):
            name = random_str(5)
            age = random.randint(0, 100)
            mongo.add_one(table, {
                "name": name,
                "age": age})

            old_user = mongo.find_one(table, {"name": name})
            new_age = random.randint(0, 100)
            mongo.update(table, {"age": new_age}, {"name": name})
            new_user = mongo.find_one(table, {"name": name})
            self.assertEqual(new_user["age"], new_age)

        mongo.delete_table(table)
        mongo.close()

    def test_add_many(self):
        mongo = self.new_db()
        table = "test_add_many"
        count = 50
        
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            users.append({"name": name, 
                "age": age})

        mongo.add_many(table, users)
        users = mongo.find_many(table, {})
        self.assertEqual(len(users), count)

        mongo.delete_table(table)
        mongo.close()
        
    def test_count(self):
        mongo = self.new_db()
        table = "test_count"
        count = 50
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            users.append({
                "name": name, 
                "age": age})
        mongo.add_many(table, users)
        self.assertEqual(mongo.count(table, {}), count)

        mongo.delete_table(table)
        mongo.close()

    def test_add_update(self):
        mongo = self.new_db()
        table = "test_add_update"
        count = 50
        for _ in range(count):
            name = random_str(20)
            age = random.randint(0, 49)
            user = {"name": name, 
                    "age": age}
            mongo.add_update(table, user)

        for _ in range(count):
            user = mongo.find_one(table, {})
            user["age"] = random.randint(50, 100)
            mongo.add_update(table, user, {"name": user["name"]})
            new_user = mongo.find_one(table, {"name": user["name"]})
            self.assertTrue(user["age"] == new_user["age"])

            new_users = mongo.find_many(table, {"name": user["name"]})
            self.assertEqual(len(new_users), 1)

            user["age"] = random.randint(101, 120)
            mongo.add_update(table, user)
            new_users = mongo.find_many(table, {"name": user["name"]})
            self.assertEqual(len(new_users), 2)
            mongo.remove(table, {"name": user["name"]})

        mongo.delete_table(table)
        mongo.close()

    def test_find_many_order(self):
        mongo = self.new_db()
        table = "test_find_many_order"
        count = 50
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = {"name": name, 
                    "age": age}
            users.append(user)
            mongo.add_one(table, user)

        users = mongo.find_many(table, {}, orderby="age")

        old_age = 0
        for user in users:
            self.assertGreaterEqual(user["age"], old_age)
            old_age = user["age"]

        mongo.delete_table(table)
        mongo.close()
    
    def test_find_one_order(self):
        mongo = self.new_db()
        table = "test_find_one_order"
        count = 50
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = {"name": name, 
                    "age": age}
            users.append(user)
            mongo.add_one(table, user)

        old_age = 0
        for _ in range(count):
            user = mongo.find_one(table, {}, orderby="age")
            self.assertGreaterEqual(user["age"], old_age)
            old_age = user["age"]

        mongo.delete_table(table)
        mongo.close()

    def test_create_db(self):
        mongo = MongoDB()
        mongo.connect()
        with self.assertRaises(DatabaseWarning):
            mongo.create_db("some_db")
        mongo.close()

    def test_is_open(self):
        mongo = MongoDB()
        self.assertFalse(mongo.is_open)
        mongo.connect()
        self.assertTrue(mongo.is_open)
        mongo.close()

    def test_connect_twice(self):
        mongo = MongoDB()
        mongo.connect()
        with self.assertRaises(DatabaseError):
            mongo.connect()
        mongo.close()

    def test_find_gt_ge_lt_le(self):
        mongo = self.new_db()
        table = "test_find_gt_ge_lt_le"
        count = 200
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = {"name": name, 
                    "age": age}
            users.append(user)
            mongo.add_one(table, user)

        users_gt_50 = mongo.find_many(table, {"age": MongoDB.gt(50)})
        for user in users_gt_50:
            self.assertGreater(user["age"], 50)

        users_ge_50 = mongo.find_many(table, {"age": MongoDB.ge(50)})
        for user in users_ge_50:
            self.assertGreaterEqual(user["age"], 50)

        users_lt_50 = mongo.find_many(table, {"age": MongoDB.lt(50)})
        for user in users_lt_50:
            self.assertLess(user["age"], 50)

        users_le_50 = mongo.find_many(table, {"age": MongoDB.le(50)})
        for user in users_le_50:
            self.assertLessEqual(user["age"], 50)

        mongo.delete_table(table)
        mongo.close()
    
    def test_type_checker(self):
        mongo = self.new_db()
        table = "test_find_one_order"
        count = 5
        users = []
        for _ in range(count):
            name = random_str(5)
            age = random.randint(0, 100)
            user = [("name", name),
                    ("age", age)]
            users.append(user)
            with self.assertRaises(TypeError):
                mongo.add(table, user)
            
            with self.assertRaises(TypeError):
                mongo.add_many(table, {})

        old_age = 0
        for _ in range(count):
            with self.assertRaises(TypeError):
                user = mongo.find_one(table, [], orderby="age")

        mongo.delete_table(table)
        mongo.close()