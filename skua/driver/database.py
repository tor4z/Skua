import MySQLdb
from exception import DBNotConnExp 

class Database(object):
    def __init__(self):
        self.__conn = None
        self.__cur = None

    def conn(self, host = "", port = "", user = "", passwd = "", db = ""):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__passwd = passwd
        self.__db = db
        if self.__conn is not None:
            self.__conn = MySQLdb.connect(
                host = self.__host,
                port = self.__port,
                user = self.__user,
                db = self.__db,
            )

    @property
    def __cursor(self):
        if self.__cur is not None:
            self.__cur = self.__conn.cursor()
        return self.__cur

    def execute(self, cmd):
        if self.__conn is not None:
            self.__cursor.execute(cmd)
            self.__conn.commit()
        else:
            raise DBNotConnExp

    def executemany(self, cmd, args):
        if self.__conn is not None:
            self.__cursor.executemany(cmd, args)
            self.__conn.commit()
        else:
            raise DBNotConnExp

    def close(self):
        self.__cursor.close()
        self.__conn.close()
