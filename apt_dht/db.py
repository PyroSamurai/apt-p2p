
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
sqlite.enable_callback_tracebacks(True)

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
        c.execute("CREATE TABLE files (path TEXT PRIMARY KEY, hash KHASH, urldir INTEGER, dirlength INTEGER, size NUMBER, mtime NUMBER, refreshed TIMESTAMP)")
        c.execute("CREATE INDEX files_urldir ON files(urldir)")
        c.execute("CREATE INDEX files_refreshed ON files(refreshed)")
        c.execute("CREATE TABLE dirs (urldir INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT)")
        c.execute("CREATE INDEX dirs_path ON dirs(path)")
        c.close()
        self.conn.commit()

    def _removeChanged(self, path, row):
        res = None
        if row:
            try:
                stat = os.stat(path)
            except:
                stat = None
            if stat:
                res = (row['size'] == stat.st_size and row['mtime'] == stat.st_mtime)
            if not res:
                c = self.conn.cursor()
                c.execute("DELETE FROM files WHERE path = ?", (path, ))
                self.conn.commit()
                c.close()
        return res
        
    def storeFile(self, path, hash, directory):
        """Store or update a file in the database.
        
        @return: the urlpath to access the file, and whether a
            new url top-level directory was needed
        """
        path = os.path.abspath(path)
        directory = os.path.abspath(directory)
        assert path.startswith(directory)
        stat = os.stat(path)
        c = self.conn.cursor()
        c.execute("SELECT dirs.urldir AS urldir, dirs.path AS directory FROM dirs JOIN files USING (urldir) WHERE files.path = ?", (path, ))
        row = c.fetchone()
        if row and directory == row['directory']:
            c.execute("UPDATE files SET hash = ?, size = ?, mtime = ?, refreshed = ?", 
                      (khash(hash), stat.st_size, stat.st_mtime, datetime.now()))
            newdir = False
            urldir = row['urldir']
        else:
            urldir, newdir = self.findDirectory(directory)
            c.execute("INSERT OR REPLACE INTO files VALUES(?, ?, ?, ?, ?, ?, ?)",
                      (path, khash(hash), urldir, len(directory), stat.st_size, stat.st_mtime, datetime.now()))
        self.conn.commit()
        c.close()
        return '/~' + str(urldir) + path[len(directory):], newdir
        
    def getFile(self, path):
        """Get a file from the database.
        
        If it has changed or is missing, it is removed from the database.
        
        @return: dictionary of info for the file, False if changed, or
            None if not in database or missing
        """
        path = os.path.abspath(path)
        c = self.conn.cursor()
        c.execute("SELECT hash, urldir, dirlength, size, mtime FROM files WHERE path = ?", (path, ))
        row = c.fetchone()
        res = self._removeChanged(path, row)
        if res:
            res = {}
            res['hash'] = row['hash']
            res['urlpath'] = '/~' + str(row['urldir']) + path[row['dirlength']:]
        c.close()
        return res
        
    def isUnchanged(self, path):
        """Check if a file in the file system has changed.
        
        If it has changed, it is removed from the table.
        
        @return: True if unchanged, False if changed, None if not in database
        """
        path = os.path.abspath(path)
        c = self.conn.cursor()
        c.execute("SELECT size, mtime FROM files WHERE path = ?", (path, ))
        row = c.fetchone()
        return self._removeChanged(path, row)

    def refreshFile(self, path):
        """Refresh the publishing time of a file.
        
        If it has changed or is missing, it is removed from the table.
        
        @return: True if unchanged, False if changed, None if not in database
        """
        path = os.path.abspath(path)
        c = self.conn.cursor()
        c.execute("SELECT size, mtime FROM files WHERE path = ?", (path, ))
        row = c.fetchone()
        res = self._removeChanged(path, row)
        if res:
            c.execute("UPDATE files SET refreshed = ? WHERE path = ?", (datetime.now(), path))
        return res
    
    def expiredFiles(self, expireAfter):
        """Find files that need refreshing after expireAfter seconds.
        
        Also removes any entries from the table that no longer exist.
        
        @return: dictionary with keys the hashes, values a list of url paths
        """
        t = datetime.now() - timedelta(seconds=expireAfter)
        c = self.conn.cursor()
        c.execute("SELECT path, hash, urldir, dirlength, size, mtime FROM files WHERE refreshed < ?", (t, ))
        row = c.fetchone()
        expired = {}
        while row:
            res = self._removeChanged(row['path'], row)
            if res:
                expired.setdefault(row['hash'], []).append('/~' + str(row['urldir']) + row['path'][row['dirlength']:])
            row = c.fetchone()
        c.close()
        return expired
        
    def removeUntrackedFiles(self, dirs):
        """Find files that are no longer tracked and so should be removed.
        
        Also removes the entries from the table.
        
        @return: list of files that were removed
        """
        assert len(dirs) >= 1
        newdirs = []
        sql = "WHERE"
        for dir in dirs:
            newdirs.append(os.path.abspath(dir) + os.sep + '*')
            sql += " path NOT GLOB ? AND"
        sql = sql[:-4]

        c = self.conn.cursor()
        c.execute("SELECT path FROM files " + sql, newdirs)
        row = c.fetchone()
        removed = []
        while row:
            removed.append(row['path'])
            row = c.fetchone()

        if removed:
            c.execute("DELETE FROM files " + sql, newdirs)
        self.conn.commit()
        return removed
    
    def findDirectory(self, directory):
        """Store or update a directory in the database.
        
        @return: the index of the url directory, and whether it is new or not
        """
        directory = os.path.abspath(directory)
        c = self.conn.cursor()
        c.execute("SELECT min(urldir) AS urldir FROM dirs WHERE path = ?", (directory, ))
        row = c.fetchone()
        c.close()
        if row['urldir']:
            return row['urldir'], False

        # Not found, need to add a new one
        c = self.conn.cursor()
        c.execute("INSERT INTO dirs (path) VALUES (?)", (directory, ))
        self.conn.commit()
        urldir = c.lastrowid
        c.close()
        return urldir, True
        
    def getAllDirectories(self):
        """Get all the current directories avaliable."""
        c = self.conn.cursor()
        c.execute("SELECT urldir, path FROM dirs")
        row = c.fetchone()
        dirs = {}
        while row:
            dirs['~' + str(row['urldir'])] = row['path']
            row = c.fetchone()
        c.close()
        return dirs
    
    def reconcileDirectories(self):
        """Remove any unneeded directories by checking which are used by files."""
        c = self.conn.cursor()
        c.execute('DELETE FROM dirs WHERE urldir NOT IN (SELECT DISTINCT urldir FROM files)')
        self.conn.commit()
        return bool(c.rowcount)
        
    def close(self):
        self.conn.close()

class TestDB(unittest.TestCase):
    """Tests for the khashmir database."""
    
    timeout = 5
    db = '/tmp/khashmir.db'
    path = '/tmp/khashmir.test'
    hash = '\xca\xec\xb8\x0c\x00\xe7\x07\xf8~])\x8f\x9d\xe5_B\xff\x1a\xc4!'
    directory = '/tmp/'
    urlpath = '/~1/khashmir.test'
    dirs = ['/tmp/apt-dht/top1', '/tmp/apt-dht/top2/sub1', '/tmp/apt-dht/top2/sub2/']

    def setUp(self):
        f = open(self.path, 'w')
        f.write('fgfhds')
        f.close()
        os.utime(self.path, None)
        self.store = DB(self.db)
        self.store.storeFile(self.path, self.hash, self.directory)

    def test_getFile(self):
        res = self.store.getFile(self.path)
        self.failUnless(res)
        self.failUnlessEqual(res['hash'], self.hash)
        self.failUnlessEqual(res['urlpath'], self.urlpath)
        
    def test_getAllDirectories(self):
        res = self.store.getAllDirectories()
        self.failUnless(res)
        self.failUnlessEqual(len(res.keys()), 1)
        self.failUnlessEqual(res.keys()[0], '~1')
        self.failUnlessEqual(res['~1'], os.path.abspath(self.directory))
        
    def test_isUnchanged(self):
        res = self.store.isUnchanged(self.path)
        self.failUnless(res)
        sleep(2)
        os.utime(self.path, None)
        res = self.store.isUnchanged(self.path)
        self.failUnless(res == False)
        os.unlink(self.path)
        res = self.store.isUnchanged(self.path)
        self.failUnless(res == None)
        
    def test_expiry(self):
        res = self.store.expiredFiles(1)
        self.failUnlessEqual(len(res.keys()), 0)
        sleep(2)
        res = self.store.expiredFiles(1)
        self.failUnlessEqual(len(res.keys()), 1)
        self.failUnlessEqual(res.keys()[0], self.hash)
        self.failUnlessEqual(len(res[self.hash]), 1)
        self.failUnlessEqual(res[self.hash][0], self.urlpath)
        res = self.store.refreshFile(self.path)
        self.failUnless(res)
        res = self.store.expiredFiles(1)
        self.failUnlessEqual(len(res.keys()), 0)
        
    def build_dirs(self):
        for dir in self.dirs:
            path = os.path.join(dir, self.path[1:])
            os.makedirs(os.path.dirname(path))
            f = open(path, 'w')
            f.write(path)
            f.close()
            os.utime(path, None)
            self.store.storeFile(path, self.hash, dir)
    
    def test_removeUntracked(self):
        self.build_dirs()
        res = self.store.removeUntrackedFiles(self.dirs)
        self.failUnlessEqual(len(res), 1, 'Got removed paths: %r' % res)
        self.failUnlessEqual(res[0], self.path, 'Got removed paths: %r' % res)
        res = self.store.removeUntrackedFiles(self.dirs)
        self.failUnlessEqual(len(res), 0, 'Got removed paths: %r' % res)
        res = self.store.removeUntrackedFiles(self.dirs[1:])
        self.failUnlessEqual(len(res), 1, 'Got removed paths: %r' % res)
        self.failUnlessEqual(res[0], os.path.join(self.dirs[0], self.path[1:]), 'Got removed paths: %r' % res)
        res = self.store.removeUntrackedFiles(self.dirs[:1])
        self.failUnlessEqual(len(res), 2, 'Got removed paths: %r' % res)
        self.failUnlessIn(os.path.join(self.dirs[1], self.path[1:]), res, 'Got removed paths: %r' % res)
        self.failUnlessIn(os.path.join(self.dirs[2], self.path[1:]), res, 'Got removed paths: %r' % res)
        
    def test_reconcileDirectories(self):
        self.build_dirs()
        res = self.store.getAllDirectories()
        self.failUnless(res)
        self.failUnlessEqual(len(res.keys()), 4)
        res = self.store.reconcileDirectories()
        self.failUnlessEqual(res, False)
        res = self.store.getAllDirectories()
        self.failUnless(res)
        self.failUnlessEqual(len(res.keys()), 4)
        res = self.store.removeUntrackedFiles(self.dirs)
        res = self.store.reconcileDirectories()
        self.failUnlessEqual(res, True)
        res = self.store.getAllDirectories()
        self.failUnless(res)
        self.failUnlessEqual(len(res.keys()), 3)
        res = self.store.removeUntrackedFiles(self.dirs[:1])
        res = self.store.reconcileDirectories()
        self.failUnlessEqual(res, True)
        res = self.store.getAllDirectories()
        self.failUnless(res)
        self.failUnlessEqual(len(res.keys()), 1)
        res = self.store.removeUntrackedFiles(['/what'])
        res = self.store.reconcileDirectories()
        self.failUnlessEqual(res, True)
        res = self.store.getAllDirectories()
        self.failUnlessEqual(len(res.keys()), 0)
        
    def tearDown(self):
        for root, dirs, files in os.walk('/tmp/apt-dht', topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        self.store.close()
        os.unlink(self.db)
