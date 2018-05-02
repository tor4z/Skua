import unittest
import random
from skua.adapter.mongo import MongoDB
from skua.adapter.database import DatabaseWarning

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