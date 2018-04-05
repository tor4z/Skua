from exception import DBNotTabExp

class DataTable:
    def __init__(self, conn, tab=""):
        self.__conn = conn
        self.__tab  = tab

    def __check_tab(self, tab):
        self.set_tab(tab)
        if not self.__tab:
            raise DBNotTabExp

    def __fields2str(self, fields, sep=" "):
        field_str = ""
        for it in fields:
            field_str += "{0}{1}{2},".format(it, sep, fields[it])
        field_str = field_str[:-1]
        return field_str

    def __execute(self, cmd):
        return self.__conn.execte(cmd)

    def create(self, tab, fields):
        cmd = "CREATE TABLE {0}({1})".format(tab, 
                                             self.__fields2str(fields))
        self.__execte(cmd)

    def set_tab(self, tab)
        if tab:
            self.__tab = tab

    def __find_str(self, fields, orderby, asc):
        if orderby:
            orderby_str = "ORDER BY {0}".format(",".join(orderby))
        else:
            orderby_str = ""

        if asc:
            order_str = "ASC"
        else:
            order_str = "DESC"

        return "SELECT * FROM {0} WHERE {1} {2} {3}".format(self.__tab, 
                                                   self.__fields2str(fields, sep = "="),
                                                   orderby_str,
                                                   order_str)

    def find_one(self, fields, tab="", orderby="", asc=True):
        self.__check_tab(tab)
        ret = self.__execte(self.__find_str(fields, orderby, asc))
        return ret.fetchone()

    def find_many(self, fields, tab="", orderby="", asc=True, size=0):
        self.__check_tab(tab)
        ret = self.__execte(self.__find_str(fields, orderby, asc))
        if size is 0:
            result = ret.fetchall()
        else:
            result = ret.fetchmany(size = size)
        return result

    def find_all(self, *args, **kwargs):
        return self.find_many(size = 0, *args, **kwargs)

    def add(self, tab="", fields):
        self.__check_tab(tab)
        cmd = "INSERT INTO {0} VALUES({1})".format(self.__tab,
                                                   ",".join(fields))
        self.__execte(cmd)

    def remove(self, fields, tab=""):
        self.__check_tab(tab)
        cmd = "DElETE FROM {0} WHERE {1}".format(self.__tab, 
                                                 self.__fields2str(fields, sep= "="))
        self.__execte(cmd)

    def update(self where, update, tab=""):
        self.__check_tab(tab)
        cmd = "UPDATE FROM {0} SET {1} WHERE {2}".format(self.__tab, 
                                                 self.__fields2str(update, sep = "=")
                                                 self.__fields2str(where, sep = "="))
        self.__execte(cmd)