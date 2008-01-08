
from time import time
from pysqlite2 import dbapi2 as sqlite
import os

class DBExcept(Exception):
    pass

class DB:
    """Database access for storing persistent data."""
    
    def __init__(self, db):
        self.db = db
        try:
            os.stat(db)
        except OSError:
            self._createNewDB(db)
        else:
            self._loadDB(db)
        self.conn.text_factory = str
        
    def _loadDB(self, db):
        try:
            self.conn = sqlite.connect(database=db, detect_types=sqlite.PARSE_DECLTYPES)
        except:
            import traceback
            raise DBExcept, "Couldn't open DB", traceback.format_exc()
        
    def _createNewDB(self, db):
        self.conn = sqlite.connect(database=db)
        c = self.conn.cursor()
        c.execute("CREATE TABLE kv (key TEXT, value TEXT, time TIMESTAMP, PRIMARY KEY (key, value))")
        c.execute("CREATE INDEX kv_key ON kv(key)")
        c.execute("CREATE INDEX kv_timestamp ON kv(time)")
        c.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, host TEXT, port NUMBER)")
        c.execute("CREATE TABLE self (num NUMBER PRIMARY KEY, id TEXT)")
        self.conn.commit()

    def getSelfNode(self):
        c = self.conn.cursor()
        c.execute('SELECT id FROM self WHERE num = 0')
        if c.rowcount > 0:
            return c.fetchone()[0]
        else:
            return None
        
    def saveSelfNode(self, id):
        c = self.conn.cursor()
        c.execute("INSERT OR REPLACE INTO self VALUES (0, ?)", (id,))
        self.conn.commit()
        
    def dumpRoutingTable(self, buckets):
        """
            save routing table nodes to the database
        """
        c = self.conn.cursor()
        c.execute("DELETE FROM nodes WHERE id NOT NULL")
        for bucket in buckets:
            for node in bucket.l:
                c.execute("INSERT INTO nodes VALUES (?, ?, ?)", (node.id, node.host, node.port))
        self.conn.commit()
        
    def getRoutingTable(self):
        """
            load routing table nodes from database
            it's usually a good idea to call refreshTable(force=1) after loading the table
        """
        c = self.conn.cursor()
        c.execute("SELECT * FROM nodes")
        return c.fetchall()
            
    def retrieveValues(self, key):
        c = self.conn.cursor()
        c.execute("SELECT value FROM kv WHERE key = ?", (key,))
        t = c.fetchone()
        l = []
        while t:
            l.append(t[0])
            t = c.fetchone()
        return l

    def storeValue(self, key, value):
        """Store or update a key and value."""
        t = "%0.6f" % time()
        c = self.conn.cursor()
        c.execute("INSERT OR REPLACE INTO kv VALUES (?, ?, ?)", (key, value, t))
        self.conn.commit()

    def expireValues(self, expireTime):
        """Expire older values than expireTime."""
        t = "%0.6f" % expireTime
        c = self.conn.cursor()
        c.execute("DELETE FROM kv WHERE time < ?", (t, ))
        self.conn.commit()
        
    def close(self):
        self.conn.close()
