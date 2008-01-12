
from datetime import datetime, timedelta
from pysqlite2 import dbapi2 as sqlite
from binascii import a2b_base64, b2a_base64
from time import sleep
import os

from twisted.trial import unittest

assert sqlite.version_info >= (2, 1)

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
        self.conn.text_factory = str
        self.conn.row_factory = sqlite.Row
        
    def _loadDB(self, db):
        try:
            self.conn = sqlite.connect(database=db, detect_types=sqlite.PARSE_DECLTYPES)
        except:
            import traceback
            raise DBExcept, "Couldn't open DB", traceback.format_exc()
        
    def _createNewDB(self, db):
        self.conn = sqlite.connect(database=db, detect_types=sqlite.PARSE_DECLTYPES)
        c = self.conn.cursor()
        c.execute("CREATE TABLE files (path TEXT PRIMARY KEY, hash KHASH, urlpath TEXT, size NUMBER, mtime NUMBER, refreshed TIMESTAMP)")
#        c.execute("CREATE INDEX files_hash ON files(hash)")
        c.execute("CREATE INDEX files_refreshed ON files(refreshed)")
        c.execute("CREATE TABLE dirs (path TEXT PRIMARY KEY, urlpath TEXT)")
        c.close()
        self.conn.commit()

    def storeFile(self, path, hash, urlpath, refreshed):
        """Store or update a file in the database."""
        path = os.path.abspath(path)
        stat = os.stat(path)
        c = self.conn.cursor()
        c.execute("INSERT OR REPLACE INTO kv VALUES (?, ?, ?, ?, ?, ?)", 
                  (path, khash(hash), urlpath, stat.st_size, stat.st_mtime, datetime.now()))
        self.conn.commit()
        c.close()
        
    def isUnchanged(self, path):
        """Check if a file in the file system has changed.
        
        If it has changed, it is removed from the table.
        
        @return: True if unchanged, False if changed, None if not in database
        """
        path = os.path.abspath(path)
        stat = os.stat(path)
        c = self.conn.cursor()
        c.execute("SELECT size, mtime FROM files WHERE path = ?", (path, ))
        row = c.fetchone()
        res = None
        if row:
            res = (row['size'] == stat.st_size and row['mtime'] == stat.st_mtime)
            if not res:
                c.execute("DELETE FROM files WHERE path = ?", path)
                self.conn.commit()
        c.close()
        return res

    def expiredFiles(self, expireAfter):
        """Find files that need refreshing after expireAfter seconds.
        
        Also removes any entries from the table that no longer exist.
        
        @return: dictionary with keys the hashes, values a list of url paths
        """
        t = datetime.now() - timedelta(seconds=expireAfter)
        c = self.conn.cursor()
        c.execute("SELECT path, hash, urlpath FROM files WHERE refreshed < ?", (t, ))
        row = c.fetchone()
        expired = {}
        missing = []
        while row:
            if os.path.exists(row['path']):
                expired.setdefault(row['hash'], []).append(row['urlpath'])
            else:
                missing.append((row['path'],))
            row = c.fetchone()
        if missing:
            c.executemany("DELETE FROM files WHERE path = ?", missing)
        self.conn.commit()
        return expired
        
    def removeUntrackedFiles(self, dirs):
        """Find files that are no longer tracked and so should be removed.
        
        Also removes the entries from the table.
        
        @return: list of files that were removed
        """
        assert len(dirs) >= 1
        dirs = dirs.copy()
        sql = "WHERE"
        for i in xrange(len(dirs)):
            dirs[i] = os.path.abspath(dirs[i])
            sql += " path NOT GLOB ?/* AND"
        sql = sql[:-4]

        c = self.conn.cursor()
        c.execute("SELECT path FROM files " + sql, dirs)
        row = c.fetchone()
        removed = []
        while row:
            removed.append(row['path'])
            row = c.fetchone()

        if removed:
            c.execute("DELETE FROM files " + sql, dirs)
        self.conn.commit()
        return removed
        
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
