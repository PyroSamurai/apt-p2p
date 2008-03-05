
from datetime import datetime, timedelta
from pysqlite2 import dbapi2 as sqlite
from binascii import a2b_base64, b2a_base64
from time import sleep
import os

from twisted.python.filepath import FilePath
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
        self.db.restat(False)
        if self.db.exists():
            self._loadDB()
        else:
            self._createNewDB()
        self.conn.text_factory = str
        self.conn.row_factory = sqlite.Row
        
    def _loadDB(self):
        try:
            self.conn = sqlite.connect(database=self.db.path, detect_types=sqlite.PARSE_DECLTYPES)
        except:
            import traceback
            raise DBExcept, "Couldn't open DB", traceback.format_exc()
        
    def _createNewDB(self):
        if not self.db.parent().exists():
            self.db.parent().makedirs()
        self.conn = sqlite.connect(database=self.db.path, detect_types=sqlite.PARSE_DECLTYPES)
        c = self.conn.cursor()
        c.execute("CREATE TABLE files (path TEXT PRIMARY KEY UNIQUE, hashID INTEGER, " +
                                      "size NUMBER, mtime NUMBER)")
        c.execute("CREATE TABLE hashes (hashID INTEGER PRIMARY KEY AUTOINCREMENT, " +
                                       "hash KHASH UNIQUE, pieces KHASH, " +
                                       "piecehash KHASH, refreshed TIMESTAMP)")
        c.execute("CREATE INDEX hashes_refreshed ON hashes(refreshed)")
        c.execute("CREATE INDEX hashes_piecehash ON hashes(piecehash)")
        c.close()
        self.conn.commit()

    def _removeChanged(self, file, row):
        res = None
        if row:
            file.restat(False)
            if file.exists():
                res = (row['size'] == file.getsize() and row['mtime'] == file.getmtime())
            if not res:
                c = self.conn.cursor()
                c.execute("DELETE FROM files WHERE path = ?", (file.path, ))
                self.conn.commit()
                c.close()
        return res
        
    def storeFile(self, file, hash, pieces = ''):
        """Store or update a file in the database.
        
        @return: True if the hash was not in the database before
            (so it needs to be added to the DHT)
        """
        piecehash = ''
        if pieces:
            s = sha.new().update(pieces)
            piecehash = sha.digest()
        c = self.conn.cursor()
        c.execute("SELECT hashID, piecehash FROM hashes WHERE hash = ?", (khash(hash), ))
        row = c.fetchone()
        if row:
            assert piecehash == row['piecehash']
            new_hash = False
            hashID = row['hashID']
        else:
            c = self.conn.cursor()
            c.execute("INSERT OR REPLACE INTO hashes (hash, pieces, piecehash, refreshed) VALUES (?, ?, ?, ?)",
                      (khash(hash), khash(pieces), khash(piecehash), datetime.now()))
            self.conn.commit()
            new_hash = True
            hashID = c.lastrowid
        
        file.restat()
        c.execute("INSERT OR REPLACE INTO files (path, hashID, size, mtime) VALUES (?, ?, ?, ?)",
                  (file.path, hashID, file.getsize(), file.getmtime()))
        self.conn.commit()
        c.close()
        
        return new_hash
        
    def getFile(self, file):
        """Get a file from the database.
        
        If it has changed or is missing, it is removed from the database.
        
        @return: dictionary of info for the file, False if changed, or
            None if not in database or missing
        """
        c = self.conn.cursor()
        c.execute("SELECT hash, size, mtime, pieces FROM files JOIN hashes USING (hashID) WHERE path = ?", (file.path, ))
        row = c.fetchone()
        res = None
        if row:
            res = self._removeChanged(file, row)
            if res:
                res = {}
                res['hash'] = row['hash']
                res['size'] = row['size']
                res['pieces'] = row['pieces']
        c.close()
        return res
        
    def lookupHash(self, hash, filesOnly = False):
        """Find a file by hash in the database.
        
        If any found files have changed or are missing, they are removed
        from the database. If filesOnly is False then it will also look for
        piece string hashes if no files can be found.
        
        @return: list of dictionaries of info for the found files
        """
        c = self.conn.cursor()
        c.execute("SELECT path, size, mtime, refreshed, pieces FROM files JOIN hashes USING (hashID) WHERE hash = ?", (khash(hash), ))
        row = c.fetchone()
        files = []
        while row:
            file = FilePath(row['path'])
            res = self._removeChanged(file, row)
            if res:
                res = {}
                res['path'] = file
                res['size'] = row['size']
                res['refreshed'] = row['refreshed']
                res['pieces'] = row['pieces']
                files.append(res)
            row = c.fetchone()
            
        if not filesOnly and not files:
            c.execute("SELECT refreshed, pieces, piecehash FROM hashes WHERE piecehash = ?", (khash(hash), ))
            row = c.fetchone()
            if row:
                res = {}
                res['refreshed'] = row['refreshed']
                res['pieces'] = row['pieces']
                files.append(res)

        c.close()
        return files
        
    def isUnchanged(self, file):
        """Check if a file in the file system has changed.
        
        If it has changed, it is removed from the table.
        
        @return: True if unchanged, False if changed, None if not in database
        """
        c = self.conn.cursor()
        c.execute("SELECT size, mtime FROM files WHERE path = ?", (file.path, ))
        row = c.fetchone()
        return self._removeChanged(file, row)

    def refreshHash(self, hash):
        """Refresh the publishing time all files with a hash."""
        c = self.conn.cursor()
        c.execute("UPDATE hashes SET refreshed = ? WHERE hash = ?", (datetime.now(), khash(hash)))
        c.close()
    
    def expiredHashes(self, expireAfter):
        """Find files that need refreshing after expireAfter seconds.
        
        For each hash that needs refreshing, finds all the files with that hash.
        If the file has changed or is missing, it is removed from the table.
        
        @return: dictionary with keys the hashes, values a list of FilePaths
        """
        t = datetime.now() - timedelta(seconds=expireAfter)
        
        # First find the hashes that need refreshing
        c = self.conn.cursor()
        c.execute("SELECT hashID, hash, pieces FROM hashes WHERE refreshed < ?", (t, ))
        row = c.fetchone()
        expired = {}
        while row:
            res = expired.setdefault(row['hash'], {})
            res['hashID'] = row['hashID']
            res['hash'] = row['hash']
            res['pieces'] = row['pieces']
            row = c.fetchone()

        # Make sure there are still valid files for each hash
        for hash in expired.values():
            valid = False
            c.execute("SELECT path, size, mtime FROM files WHERE hashID = ?", (hash['hashID'], ))
            row = c.fetchone()
            while row:
                res = self._removeChanged(FilePath(row['path']), row)
                if res:
                    valid = True
                row = c.fetchone()
            if not valid:
                del expired[hash['hash']]
                c.execute("DELETE FROM hashes WHERE hashID = ?", (hash['hashID'], ))
                
        self.conn.commit()
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
            newdirs.append(dir.child('*').path)
            sql += " path NOT GLOB ? AND"
        sql = sql[:-4]

        c = self.conn.cursor()
        c.execute("SELECT path FROM files " + sql, newdirs)
        row = c.fetchone()
        removed = []
        while row:
            removed.append(FilePath(row['path']))
            row = c.fetchone()

        if removed:
            c.execute("DELETE FROM files " + sql, newdirs)
        self.conn.commit()
        return removed
    
    def close(self):
        self.conn.close()

class TestDB(unittest.TestCase):
    """Tests for the khashmir database."""
    
    timeout = 5
    db = FilePath('/tmp/khashmir.db')
    hash = '\xca\xec\xb8\x0c\x00\xe7\x07\xf8~])\x8f\x9d\xe5_B\xff\x1a\xc4!'
    directory = FilePath('/tmp/apt-dht/')
    file = FilePath('/tmp/apt-dht/khashmir.test')
    testfile = 'tmp/khashmir.test'
    dirs = [FilePath('/tmp/apt-dht/top1'),
            FilePath('/tmp/apt-dht/top2/sub1'),
            FilePath('/tmp/apt-dht/top2/sub2/')]

    def setUp(self):
        if not self.file.parent().exists():
            self.file.parent().makedirs()
        self.file.setContent('fgfhds')
        self.file.touch()
        self.store = DB(self.db)
        self.store.storeFile(self.file, self.hash)

    def test_openExistingDB(self):
        self.store.close()
        self.store = None
        sleep(1)
        self.store = DB(self.db)
        res = self.store.isUnchanged(self.file)
        self.failUnless(res)

    def test_getFile(self):
        res = self.store.getFile(self.file)
        self.failUnless(res)
        self.failUnlessEqual(res['hash'], self.hash)
        
    def test_lookupHash(self):
        res = self.store.lookupHash(self.hash)
        self.failUnless(res)
        self.failUnlessEqual(len(res), 1)
        self.failUnlessEqual(res[0]['path'].path, self.file.path)
        
    def test_isUnchanged(self):
        res = self.store.isUnchanged(self.file)
        self.failUnless(res)
        sleep(2)
        self.file.touch()
        res = self.store.isUnchanged(self.file)
        self.failUnless(res == False)
        res = self.store.isUnchanged(self.file)
        self.failUnless(res is None)
        
    def test_expiry(self):
        res = self.store.expiredHashes(1)
        self.failUnlessEqual(len(res.keys()), 0)
        sleep(2)
        res = self.store.expiredHashes(1)
        self.failUnlessEqual(len(res.keys()), 1)
        self.failUnlessEqual(res.keys()[0], self.hash)
        self.store.refreshHash(self.hash)
        res = self.store.expiredHashes(1)
        self.failUnlessEqual(len(res.keys()), 0)
        
    def build_dirs(self):
        for dir in self.dirs:
            file = dir.preauthChild(self.testfile)
            if not file.parent().exists():
                file.parent().makedirs()
            file.setContent(file.path)
            file.touch()
            self.store.storeFile(file, self.hash)
    
    def test_multipleHashes(self):
        self.build_dirs()
        res = self.store.expiredHashes(1)
        self.failUnlessEqual(len(res.keys()), 0)
        res = self.store.lookupHash(self.hash)
        self.failUnless(res)
        self.failUnlessEqual(len(res), 4)
        self.failUnlessEqual(res[0]['refreshed'], res[1]['refreshed'])
        self.failUnlessEqual(res[0]['refreshed'], res[2]['refreshed'])
        self.failUnlessEqual(res[0]['refreshed'], res[3]['refreshed'])
        sleep(2)
        res = self.store.expiredHashes(1)
        self.failUnlessEqual(len(res.keys()), 1)
        self.failUnlessEqual(res.keys()[0], self.hash)
        self.store.refreshHash(self.hash)
        res = self.store.expiredHashes(1)
        self.failUnlessEqual(len(res.keys()), 0)
    
    def test_removeUntracked(self):
        self.build_dirs()
        res = self.store.removeUntrackedFiles(self.dirs)
        self.failUnlessEqual(len(res), 1, 'Got removed paths: %r' % res)
        self.failUnlessEqual(res[0], self.file, 'Got removed paths: %r' % res)
        res = self.store.removeUntrackedFiles(self.dirs)
        self.failUnlessEqual(len(res), 0, 'Got removed paths: %r' % res)
        res = self.store.removeUntrackedFiles(self.dirs[1:])
        self.failUnlessEqual(len(res), 1, 'Got removed paths: %r' % res)
        self.failUnlessEqual(res[0], self.dirs[0].preauthChild(self.testfile), 'Got removed paths: %r' % res)
        res = self.store.removeUntrackedFiles(self.dirs[:1])
        self.failUnlessEqual(len(res), 2, 'Got removed paths: %r' % res)
        self.failUnlessIn(self.dirs[1].preauthChild(self.testfile), res, 'Got removed paths: %r' % res)
        self.failUnlessIn(self.dirs[2].preauthChild(self.testfile), res, 'Got removed paths: %r' % res)
        
    def tearDown(self):
        self.directory.remove()
        self.store.close()
        self.db.remove()

