
from datetime import datetime, timedelta
from pysqlite2 import dbapi2 as sqlite
from binascii import a2b_base64, b2a_base64
from time import sleep
import os

from twisted.trial import unittest

class DBExcept(Exception):
    pass

class khash(str):
    """Dummy class to convert all hashes to base64 for storing in the DB."""
    
sqlite.register_adapter(khash, b2a_base64)
sqlite.register_converter("KHASH", a2b_base64)
sqlite.register_converter("khash", a2b_base64)

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
        if sqlite.version_info < (2, 1):
            sqlite.register_converter("TEXT", str)
            sqlite.register_converter("text", str)
        else:
            self.conn.text_factory = str
        
    def _loadDB(self, db):
        try:
            self.conn = sqlite.connect(database=db, detect_types=sqlite.PARSE_DECLTYPES)
        except:
            import traceback
            raise DBExcept, "Couldn't open DB", traceback.format_exc()
        
    def _createNewDB(self, db):
        self.conn = sqlite.connect(database=db, detect_types=sqlite.PARSE_DECLTYPES)
        c = self.conn.cursor()
        c.execute("CREATE TABLE kv (key KHASH, value TEXT, time TIMESTAMP, PRIMARY KEY (key, value))")
        c.execute("CREATE INDEX kv_key ON kv(key)")
        c.execute("CREATE INDEX kv_timestamp ON kv(time)")
        c.execute("CREATE TABLE nodes (id KHASH PRIMARY KEY, host TEXT, port NUMBER)")
        c.execute("CREATE TABLE self (num NUMBER PRIMARY KEY, id KHASH)")
        self.conn.commit()

    def getSelfNode(self):
        c = self.conn.cursor()
        c.execute('SELECT id FROM self WHERE num = 0')
        id = c.fetchone()
        if id:
            return id[0]
        else:
            return None
        
    def saveSelfNode(self, id):
        c = self.conn.cursor()
        c.execute("INSERT OR REPLACE INTO self VALUES (0, ?)", (khash(id),))
        self.conn.commit()
        
    def dumpRoutingTable(self, buckets):
        """
            save routing table nodes to the database
        """
        c = self.conn.cursor()
        c.execute("DELETE FROM nodes WHERE id NOT NULL")
        for bucket in buckets:
            for node in bucket.l:
                c.execute("INSERT INTO nodes VALUES (?, ?, ?)", (khash(node.id), node.host, node.port))
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
        c.execute("SELECT value FROM kv WHERE key = ?", (khash(key),))
        t = c.fetchone()
        l = []
        while t:
            l.append(t[0])
            t = c.fetchone()
        return l

    def storeValue(self, key, value):
        """Store or update a key and value."""
        c = self.conn.cursor()
        c.execute("INSERT OR REPLACE INTO kv VALUES (?, ?, ?)", (khash(key), value, datetime.now()))
        self.conn.commit()

    def expireValues(self, expireAfter):
        """Expire older values after expireAfter seconds."""
        t = datetime.now() - timedelta(seconds=expireAfter)
        c = self.conn.cursor()
        c.execute("DELETE FROM kv WHERE time < ?", (t, ))
        self.conn.commit()
        
    def close(self):
        self.conn.close()

class TestDB(unittest.TestCase):
    """Tests for the khashmir database."""
    
    timeout = 5
    db = '/tmp/khashmir.db'
    key = '\xca\xec\xb8\x0c\x00\xe7\x07\xf8~])\x8f\x9d\xe5_B\xff\x1a\xc4!'

    def setUp(self):
        self.store = DB(self.db)

    def test_selfNode(self):
        self.store.saveSelfNode(self.key)
        self.failUnlessEqual(self.store.getSelfNode(), self.key)
        
    def test_Value(self):
        self.store.storeValue(self.key, 'foobar')
        val = self.store.retrieveValues(self.key)
        self.failUnlessEqual(len(val), 1)
        self.failUnlessEqual(val[0], 'foobar')
        
    def test_expireValues(self):
        self.store.storeValue(self.key, 'foobar')
        sleep(2)
        self.store.storeValue(self.key, 'barfoo')
        self.store.expireValues(1)
        val = self.store.retrieveValues(self.key)
        self.failUnlessEqual(len(val), 1)
        self.failUnlessEqual(val[0], 'barfoo')
        
    def test_RoutingTable(self):
        class dummy:
            id = self.key
            host = "127.0.0.1"
            port = 9977
            def contents(self):
                return (self.id, self.host, self.port)
        dummy2 = dummy()
        dummy2.id = '\xaa\xbb\xcc\x0c\x00\xe7\x07\xf8~])\x8f\x9d\xe5_B\xff\x1a\xc4!'
        dummy2.host = '205.23.67.124'
        dummy2.port = 12345
        class bl:
            def __init__(self):
                self.l = []
        bl1 = bl()
        bl1.l.append(dummy())
        bl2 = bl()
        bl2.l.append(dummy2)
        buckets = [bl1, bl2]
        self.store.dumpRoutingTable(buckets)
        rt = self.store.getRoutingTable()
        self.failUnlessIn(dummy().contents(), rt)
        self.failUnlessIn(dummy2.contents(), rt)
        
    def tearDown(self):
        self.store.close()
        os.unlink(self.db)
