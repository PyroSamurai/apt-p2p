
"""An sqlite database for storing persistent files and hashes."""

from datetime import datetime, timedelta
from pysqlite2 import dbapi2 as sqlite
from binascii import a2b_base64, b2a_base64
from time import sleep
import os

from twisted.python.filepath import FilePath
from twisted.trial import unittest

assert sqlite.version_info >= (2, 1)

class DBExcept(Exception):
    """An error occurred in accessing the database."""
    pass

class khash(str):
    """Dummy class to convert all hashes to base64 for storing in the DB."""

# Initialize the database to work with 'khash' objects (binary strings)
sqlite.register_adapter(khash, b2a_base64)
sqlite.register_converter("KHASH", a2b_base64)
sqlite.register_converter("khash", a2b_base64)
sqlite.enable_callback_tracebacks(True)

class DB:
    """An sqlite database for storing persistent files and hashes.
    
    @type db: L{twisted.python.filepath.FilePath}
    @ivar db: the database file to use
    @type conn: L{pysqlite2.dbapi2.Connection}
    @ivar conn: an open connection to the sqlite database
    """
    
    def __init__(self, db):
        """Load or create the database file.
        
        @type db: L{twisted.python.filepath.FilePath}
        @param db: the database file to use
        """
        self.db = db
        self.db.restat(False)
        if self.db.exists():
            self._loadDB()
        else:
            self._createNewDB()
        self.conn.text_factory = str
        self.conn.row_factory = sqlite.Row
        
    def _loadDB(self):
        """Open a new connection to the existing database file"""
        try:
            self.conn = sqlite.connect(database=self.db.path, detect_types=sqlite.PARSE_DECLTYPES)
        except:
            import traceback
            raise DBExcept, "Couldn't open DB", traceback.format_exc()
        
    def _createNewDB(self):
        """Open a connection to a new database and create the necessary tables."""
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
        """If the file has changed or is missing, remove it from the DB.
        
        @type file: L{twisted.python.filepath.FilePath}
        @param file: the file to check
        @type row: C{dictionary}-like object
        @param row: contains the expected 'size' and 'mtime' of the file
        @rtype: C{boolean}
        @return: True if the file is unchanged, False if it is changed,
            and None if it is missing
        """
        res = None
        if row:
            file.restat(False)
            if file.exists():
                # Compare the current with the expected file properties
                res = (row['size'] == file.getsize() and row['mtime'] == file.getmtime())
            if not res:
                # Remove the file from the database
                c = self.conn.cursor()
                c.execute("DELETE FROM files WHERE path = ?", (file.path, ))
                self.conn.commit()
                c.close()
        return res
        
    def storeFile(self, file, hash, pieces = ''):
        """Store or update a file in the database.
        
        @type file: L{twisted.python.filepath.FilePath}
        @param file: the file to check
        @type hash: C{string}
        @param hash: the hash of the file
        @type pieces: C{string}
        @param pieces: the concatenated list of the hashes of the pieces of
            the file (optional, defaults to the empty string)
        @return: True if the hash was not in the database before
            (so it needs to be added to the DHT)
        """
        # Hash the pieces to get the piecehash
        piecehash = ''
        if pieces:
            s = sha.new().update(pieces)
            piecehash = sha.digest()
            
        # Check the database for the hash
        c = self.conn.cursor()
        c.execute("SELECT hashID, piecehash FROM hashes WHERE hash = ?", (khash(hash), ))
        row = c.fetchone()
        if row:
            assert piecehash == row['piecehash']
            new_hash = False
            hashID = row['hashID']
        else:
            # Add the new hash to the database
            c = self.conn.cursor()
            c.execute("INSERT OR REPLACE INTO hashes (hash, pieces, piecehash, refreshed) VALUES (?, ?, ?, ?)",
                      (khash(hash), khash(pieces), khash(piecehash), datetime.now()))
            self.conn.commit()
            new_hash = True
            hashID = c.lastrowid

        # Add the file to the database
        file.restat()
        c.execute("INSERT OR REPLACE INTO files (path, hashID, size, mtime) VALUES (?, ?, ?, ?)",
                  (file.path, hashID, file.getsize(), file.getmtime()))
        self.conn.commit()
        c.close()
        
        return new_hash
        
    def getFile(self, file):
        """Get a file from the database.
        
        If it has changed or is missing, it is removed from the database.
        
        @type file: L{twisted.python.filepath.FilePath}
        @param file: the file to check
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
        # Try to find the hash in the files table
        c = self.conn.cursor()
        c.execute("SELECT path, size, mtime, refreshed, pieces FROM files JOIN hashes USING (hashID) WHERE hash = ?", (khash(hash), ))
        row = c.fetchone()
        files = []
        while row:
            # Save the file to the list of found files
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
            # No files were found, so check the piecehashes as well
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
        
        If it has changed, it is removed from the database.
        
        @return: True if unchanged, False if changed, None if not in database
        """
        c = self.conn.cursor()
        c.execute("SELECT size, mtime FROM files WHERE path = ?", (file.path, ))
        row = c.fetchone()
        return self._removeChanged(file, row)

    def refreshHash(self, hash):
        """Refresh the publishing time of a hash."""
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
        
        # Find all the hashes that need refreshing
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
                # Remove hashes for which no files are still available
                del expired[hash['hash']]
                c.execute("DELETE FROM hashes WHERE hashID = ?", (hash['hashID'], ))
                
        self.conn.commit()
        c.close()
        
        return expired
        
    def removeUntrackedFiles(self, dirs):
        """Remove files that are no longer tracked by the program.
        
        @type dirs: C{list} of L{twisted.python.filepath.FilePath}
        @param dirs: a list of the directories that we are tracking
        @return: list of files that were removed
        """
        assert len(dirs) >= 1
        
        # Create a list of globs and an SQL statement for the directories
        newdirs = []
        sql = "WHERE"
        for dir in dirs:
            newdirs.append(dir.child('*').path)
            sql += " path NOT GLOB ? AND"
        sql = sql[:-4]

        # Get a listing of all the files that will be removed
        c = self.conn.cursor()
        c.execute("SELECT path FROM files " + sql, newdirs)
        row = c.fetchone()
        removed = []
        while row:
            removed.append(FilePath(row['path']))
            row = c.fetchone()

        # Delete all the removed files from the database
        if removed:
            c.execute("DELETE FROM files " + sql, newdirs)
        self.conn.commit()

        return removed
    
    def close(self):
        """Close the database connection."""
        self.conn.close()

class TestDB(unittest.TestCase):
    """Tests for the khashmir database."""
    
    timeout = 5
    db = FilePath('/tmp/khashmir.db')
    hash = '\xca\xec\xb8\x0c\x00\xe7\x07\xf8~])\x8f\x9d\xe5_B\xff\x1a\xc4!'
    directory = FilePath('/tmp/apt-p2p/')
    file = FilePath('/tmp/apt-p2p/khashmir.test')
    testfile = 'tmp/khashmir.test'
    dirs = [FilePath('/tmp/apt-p2p/top1'),
            FilePath('/tmp/apt-p2p/top2/sub1'),
            FilePath('/tmp/apt-p2p/top2/sub2/')]

    def setUp(self):
        if not self.file.parent().exists():
            self.file.parent().makedirs()
        self.file.setContent('fgfhds')
        self.file.touch()
        self.store = DB(self.db)
        self.store.storeFile(self.file, self.hash)

    def test_openExistingDB(self):
        """Tests opening an existing database."""
        self.store.close()
        self.store = None
        sleep(1)
        self.store = DB(self.db)
        res = self.store.isUnchanged(self.file)
        self.failUnless(res)

    def test_getFile(self):
        """Tests retrieving a file from the database."""
        res = self.store.getFile(self.file)
        self.failUnless(res)
        self.failUnlessEqual(res['hash'], self.hash)
        
    def test_lookupHash(self):
        """Tests looking up a hash in the database."""
        res = self.store.lookupHash(self.hash)
        self.failUnless(res)
        self.failUnlessEqual(len(res), 1)
        self.failUnlessEqual(res[0]['path'].path, self.file.path)
        
    def test_isUnchanged(self):
        """Tests checking if a file in the database is unchanged."""
        res = self.store.isUnchanged(self.file)
        self.failUnless(res)
        sleep(2)
        self.file.touch()
        res = self.store.isUnchanged(self.file)
        self.failUnless(res == False)
        res = self.store.isUnchanged(self.file)
        self.failUnless(res is None)
        
    def test_expiry(self):
        """Tests retrieving the files from the database that have expired."""
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
        """Tests looking up a hash with multiple files in the database."""
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
        """Tests removing untracked files from the database."""
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

