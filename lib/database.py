import MySQLdb

from config import DB_CONFIG


class Database(object):

    persistent_connections = dict()
    persistent_cursors = dict()

    def __init__(self, db_name='', persistent=False):
        """
        
        :param db_name: имя базы данных, так как оно прописано в config 
        :param persistent:
        :type persistent: bool
        """
        super(Database, self).__init__()
        self.persistent = persistent
        self.db_name = db_name
        self._conn = None
        self.connect()

    def connect(self):
        if self.persistent:
            try:
                self._conn = self.persistent_connections[self.db_name]
            except KeyError:
                self._conn = self.persistent_connections[self.db_name] = MySQLdb.connect(**DB_CONFIG[self.db_name])
                self._conn.autocommit(True)
                self.persistent_cursors[self.db_name] = self._conn.cursor()
        else:
            self._conn = MySQLdb.connect(**DB_CONFIG[self.db_name])
            self._conn.autocommit(True)

        return self._conn

    def close(self):
        """Closes connection"""
        self._conn.close()

    def __call__(self, *args, **kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.persistent:
            self.close()

    def cursor(self):
        if self.persistent:
            cur = self.persistent_cursors[self.db_name]
        else:
            cur = self._conn.cursor()

        return cur