import unittest
import random
from skua.driver.mysql import MySQLDB

TEST_DB = "skua_test"
_STR = "asbcdefhijklmnopqrstuvwxyz_"

def random_str(k):
    return "".join(random.choices(_STR, k=5))

class TestMySQL(unittest.TestCase):
    def new_db(self):
        mysql = MySQLDB()
        mysql.connect(passwd="")
        try:
            mysql.create_db(TEST_DB)
        except:
            pass
        mysql.select_db(TEST_DB)
        return mysql

    def test_exit_table(self):
        mysql = self.new_db()
        self.assertFalse(mysql.table_exit("some_table_unexist"))
        self.assertTrue(mysql.table_exit("user"))
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