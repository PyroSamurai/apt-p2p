
"""Hash and store hash information for a file.

@var PIECE_SIZE: the piece size to use for hashing pieces of files

"""

from binascii import b2a_hex, a2b_hex
import sys

from twisted.internet import threads, defer
from twisted.trial import unittest

PIECE_SIZE = 512*1024

class HashError(ValueError):
    """An error has occurred while hashing a file."""
    
class HashObject:
    """Manages hashes and hashing for a file.
    
    @ivar ORDER: the priority ordering of hashes, and how to extract them

    """

    ORDER = [ {'name': 'sha1', 
                   'length': 20,
                   'AptPkgRecord': 'SHA1Hash', 
                   'AptSrcRecord': False, 
                   'AptIndexRecord': 'SHA1',
                   'old_module': 'sha',
                   'hashlib_func': 'sha1',
                   },
              {'name': 'sha256',
                   'length': 32,
                   'AptPkgRecord': 'SHA256Hash', 
                   'AptSrcRecord': False, 
                   'AptIndexRecord': 'SHA256',
                   'hashlib_func': 'sha256',
                   },
              {'name': 'md5',
                   'length': 16,
                   'AptPkgRecord': 'MD5Hash', 
                   'AptSrcRecord': True, 
                   'AptIndexRecord': 'MD5SUM',
                   'old_module': 'md5',
                   'hashlib_func': 'md5',
                   },
            ]
    
    def __init__(self, digest = None, size = None, pieces = ''):
        """Initialize the hash object."""
        self.hashTypeNum = 0    # Use the first if nothing else matters
        if sys.version_info < (2, 5):
            # sha256 is not available in python before 2.5, remove it
            for hashType in self.ORDER:
                if hashType['name'] == 'sha256':
                    del self.ORDER[self.ORDER.index(hashType)]
                    break

        self.expHash = None
        self.expHex = None
        self.expSize = None
        self.expNormHash = None
        self.fileHasher = None
        self.pieceHasher = None
        self.fileHash = digest
        self.pieceHash = [pieces[x:x+self.ORDER[self.hashTypeNum]['length']]
                          for x in xrange(0, len(pieces), self.ORDER[self.hashTypeNum]['length'])]
        self.size = size
        self.fileHex = None
        self.fileNormHash = None
        self.done = True
        self.result = None
        
    #{ Hashing data
    def new(self, force = False):
        """Generate a new hashing object suitable for hashing a file.
        
        @param force: set to True to force creating a new object even if
            the hash has been verified already
        """
        if self.result is None or force:
            self.result = None
            self.done = False
            self.fileHasher = self._new()
            self.pieceHasher = None
            self.fileHash = None
            self.pieceHash = []
            self.size = 0
            self.fileHex = None
            self.fileNormHash = None

    def _new(self):
        """Create a new hashing object according to the hash type."""
        if sys.version_info < (2, 5):
            mod = __import__(self.ORDER[self.hashTypeNum]['old_module'], globals(), locals(), [])
            return mod.new()
        else:
            import hashlib
            func = getattr(hashlib, self.ORDER[self.hashTypeNum]['hashlib_func'])
            return func()

    def update(self, data):
        """Add more data to the file hasher."""
        if self.result is None:
            if self.done:
                raise HashError, "Already done, you can't add more data after calling digest() or verify()"
            if self.fileHasher is None:
                raise HashError, "file hasher not initialized"
            
            if not self.pieceHasher and self.size + len(data) > PIECE_SIZE:
                # Hash up to the piece size
                self.fileHasher.update(data[:(PIECE_SIZE - self.size)])
                data = data[(PIECE_SIZE - self.size):]
                self.size = PIECE_SIZE

                # Save the first piece digest and initialize a new piece hasher
                self.pieceHash.append(self.fileHasher.digest())
                self.pieceHasher = self._new()

            if self.pieceHasher:
                # Loop in case the data contains multiple pieces
                piece_size = self.size % PIECE_SIZE
                while piece_size + len(data) > PIECE_SIZE:
                    # Save the piece hash and start a new one
                    self.pieceHasher.update(data[:(PIECE_SIZE - piece_size)])
                    self.pieceHash.append(self.pieceHasher.digest())
                    self.pieceHasher = self._new()
                    
                    # Don't forget to hash the data normally
                    self.fileHasher.update(data[:(PIECE_SIZE - piece_size)])
                    data = data[(PIECE_SIZE - piece_size):]
                    self.size += PIECE_SIZE - piece_size
                    piece_size = self.size % PIECE_SIZE

                # Hash any remaining data
                self.pieceHasher.update(data)
            
            self.fileHasher.update(data)
            self.size += len(data)
        
    def hashInThread(self, file):
        """Hashes a file in a separate thread, returning a deferred that will callback with the result."""
        file.restat(False)
        if not file.exists():
            df = defer.Deferred()
            df.errback(HashError("file not found"))
            return df
        
        df = threads.deferToThread(self._hashInThread, file)
        return df
    
    def _hashInThread(self, file):
        """Hashes a file, returning itself as the result."""
        f = file.open()
        self.new(force = True)
        data = f.read(4096)
        while data:
            self.update(data)
            data = f.read(4096)
        self.digest()
        return self

    #{ Checking hashes of data
    def pieceDigests(self):
        """Get the piece hashes of the added file data."""
        self.digest()
        return self.pieceHash

    def digest(self):
        """Get the hash of the added file data."""
        if self.fileHash is None:
            if self.fileHasher is None:
                raise HashError, "you must hash some data first"
            self.fileHash = self.fileHasher.digest()
            self.done = True
            
            # Save the last piece hash
            if self.pieceHasher:
                self.pieceHash.append(self.pieceHasher.digest())
        return self.fileHash

    def hexdigest(self):
        """Get the hash of the added file data in hex format."""
        if self.fileHex is None:
            self.fileHex = b2a_hex(self.digest())
        return self.fileHex
        
    def verify(self):
        """Verify that the added file data hash matches the expected hash."""
        if self.result is None and self.fileHash is not None and self.expHash is not None:
            self.result = (self.fileHash == self.expHash and self.size == self.expSize)
        return self.result
    
    #{ Expected hash
    def expected(self):
        """Get the expected hash."""
        return self.expHash
    
    def hexexpected(self):
        """Get the expected hash in hex format."""
        if self.expHex is None and self.expHash is not None:
            self.expHex = b2a_hex(self.expHash)
        return self.expHex
    
    #{ Setting the expected hash
    def set(self, hashType, hashHex, size):
        """Initialize the hash object.
        
        @param hashType: must be one of the dictionaries from L{ORDER}
        """
        self.hashTypeNum = self.ORDER.index(hashType)    # error if not found
        self.expHex = hashHex
        self.expSize = int(size)
        self.expHash = a2b_hex(self.expHex)
        
    def setFromIndexRecord(self, record):
        """Set the hash from the cache of index file records.
        
        @type record: C{dictionary}
        @param record: keys are hash types, values are tuples of (hash, size)
        """
        for hashType in self.ORDER:
            result = record.get(hashType['AptIndexRecord'], None)
            if result:
                self.set(hashType, result[0], result[1])
                return True
        return False

    def setFromPkgRecord(self, record, size):
        """Set the hash from Apt's binary packages cache.
        
        @param record: whatever is returned by apt_pkg.GetPkgRecords()
        """
        for hashType in self.ORDER:
            hashHex = getattr(record, hashType['AptPkgRecord'], None)
            if hashHex:
                self.set(hashType, hashHex, size)
                return True
        return False
    
    def setFromSrcRecord(self, record):
        """Set the hash from Apt's source package records cache.
        
        Currently very simple since Apt only tracks MD5 hashes of source files.
        
        @type record: (C{string}, C{int}, C{string})
        @param record: the hash, size and path of the source file
        """
        for hashType in self.ORDER:
            if hashType['AptSrcRecord']:
                self.set(hashType, record[0], record[1])
                return True
        return False

class TestHashObject(unittest.TestCase):
    """Unit tests for the hash objects."""
    
    timeout = 5
    if sys.version_info < (2, 4):
        skip = "skippingme"
    
    def test_failure(self):
        """Tests that the hash object fails when treated badly."""
        h = HashObject()
        h.set(h.ORDER[0], b2a_hex('12345678901234567890'), '0')
        self.failUnlessRaises(HashError, h.digest)
        self.failUnlessRaises(HashError, h.hexdigest)
        self.failUnlessRaises(HashError, h.update, 'gfgf')
    
    def test_pieces(self):
        """Tests the hashing of large files into pieces."""
        h = HashObject()
        h.new()
        h.update('1234567890'*120*1024)
        self.failUnless(h.digest() == '1(j\xd2q\x0b\n\x91\xd2\x13\x90\x15\xa3E\xcc\xb0\x8d.\xc3\xc5')
        pieces = h.pieceDigests()
        self.failUnless(len(pieces) == 3)
        self.failUnless(pieces[0] == ',G \xd8\xbbPl\xf1\xa3\xa0\x0cW\n\xe6\xe6a\xc9\x95/\xe5')
        self.failUnless(pieces[1] == '\xf6V\xeb/\xa8\xad[\x07Z\xf9\x87\xa4\xf5w\xdf\xe1|\x00\x8e\x93')
        self.failUnless(pieces[2] == 'M[\xbf\xee\xaa+\x19\xbaV\xf699\r\x17o\xcb\x8e\xcfP\x19')
        h.new(True)
        for i in xrange(120*1024):
            h.update('1234567890')
        pieces = h.pieceDigests()
        self.failUnless(h.digest() == '1(j\xd2q\x0b\n\x91\xd2\x13\x90\x15\xa3E\xcc\xb0\x8d.\xc3\xc5')
        self.failUnless(len(pieces) == 3)
        self.failUnless(pieces[0] == ',G \xd8\xbbPl\xf1\xa3\xa0\x0cW\n\xe6\xe6a\xc9\x95/\xe5')
        self.failUnless(pieces[1] == '\xf6V\xeb/\xa8\xad[\x07Z\xf9\x87\xa4\xf5w\xdf\xe1|\x00\x8e\x93')
        self.failUnless(pieces[2] == 'M[\xbf\xee\xaa+\x19\xbaV\xf699\r\x17o\xcb\x8e\xcfP\x19')
        
    def test_sha1(self):
        """Test hashing using the SHA1 hash."""
        h = HashObject()
        found = False
        for hashType in h.ORDER:
            if hashType['name'] == 'sha1':
                found = True
                break
        self.failUnless(found == True)
        h.set(hashType, 'c722df87e1acaa64b27aac4e174077afc3623540', '19')
        h.new()
        h.update('apt-dht is the best')
        self.failUnless(h.hexdigest() == 'c722df87e1acaa64b27aac4e174077afc3623540')
        self.failUnlessRaises(HashError, h.update, 'gfgf')
        self.failUnless(h.verify() == True)
        
    def test_md5(self):
        """Test hashing using the MD5 hash."""
        h = HashObject()
        found = False
        for hashType in h.ORDER:
            if hashType['name'] == 'md5':
                found = True
                break
        self.failUnless(found == True)
        h.set(hashType, '2a586bcd1befc5082c872dcd96a01403', '19')
        h.new()
        h.update('apt-dht is the best')
        self.failUnless(h.hexdigest() == '2a586bcd1befc5082c872dcd96a01403')
        self.failUnlessRaises(HashError, h.update, 'gfgf')
        self.failUnless(h.verify() == True)
        
    def test_sha256(self):
        """Test hashing using the SHA256 hash."""
        h = HashObject()
        found = False
        for hashType in h.ORDER:
            if hashType['name'] == 'sha256':
                found = True
                break
        self.failUnless(found == True)
        h.set(hashType, '55b971f64d9772f733de03f23db39224f51a455cc5ad4c2db9d5740d2ab259a7', '19')
        h.new()
        h.update('apt-dht is the best')
        self.failUnless(h.hexdigest() == '55b971f64d9772f733de03f23db39224f51a455cc5ad4c2db9d5740d2ab259a7')
        self.failUnlessRaises(HashError, h.update, 'gfgf')
        self.failUnless(h.verify() == True)

    if sys.version_info < (2, 5):
        test_sha256.skip = "SHA256 hashes are not supported by Python until version 2.5"
