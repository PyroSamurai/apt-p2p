
from binascii import b2a_hex, a2b_hex
import sys

from twisted.trial import unittest

class HashObject:
    """Manages hashes and hashing for a file."""
    
    """The priority ordering of hashes, and how to extract them."""
    ORDER = [ {'name': 'sha1', 
                   'AptPkgRecord': 'SHA1Hash', 
                   'AptSrcRecord': False, 
                   'AptIndexRecord': 'SHA1',
                   'old_module': 'sha',
                   'hashlib_func': 'sha1',
                   },
              {'name': 'sha256',
                   'AptPkgRecord': 'SHA256Hash', 
                   'AptSrcRecord': False, 
                   'AptIndexRecord': 'SHA256',
                   'hashlib_func': 'sha256',
                   },
              {'name': 'md5',
                   'AptPkgRecord': 'MD5Hash', 
                   'AptSrcRecord': True, 
                   'AptIndexRecord': 'MD5Sum',
                   'old_module': 'md5',
                   'hashlib_func': 'md5',
                   },
            ]
    
    def __init__(self):
        self.hashTypeNum = 0    # Use the first if nothing else matters
        self.expHash = None
        self.expHex = None
        self.expSize = None
        self.expNormHash = None
        self.fileHasher = None
        self.fileHash = None
        self.fileHex = None
        self.fileNormHash = None
        self.done = True
        self.result = None
        if sys.version_info < (2, 5):
            # sha256 is not available in python before 2.5, remove it
            for hashType in self.ORDER:
                if hashType['name'] == 'sha256':
                    del self.ORDER[self.ORDER.index(hashType)]
                    break
        
    def _norm_hash(self, hashString, bits=None, bytes=None):
        if bits is not None:
            bytes = (bits - 1) // 8 + 1
        else:
            assert bytes is not None, "you must specify one of bits or bytes"
        if len(hashString) < bytes:
            hashString = hashString + '\000'*(bytes - len(hashString))
        elif len(hashString) > bytes:
            hashString = hashString[:bytes]
        return hashString

    #### Methods for returning the expected hash
    def expected(self):
        """Get the expected hash."""
        return self.expHash
    
    def hexexpected(self):
        """Get the expected hash in hex format."""
        if self.expHex is None and self.expHash is not None:
            self.expHex = b2a_hex(self.expHash)
        return self.expHex
    
    def normexpected(self, bits=None, bytes=None):
        """Normalize the binary hash for the given length.
        
        You must specify one of bits or bytes.
        """
        if self.expNormHash is None and self.expHash is not None:
            self.expNormHash = self._norm_hash(self.expHash, bits, bytes)
        return self.expNormHash

    #### Methods for hashing data
    def new(self, force = False):
        """Generate a new hashing object suitable for hashing a file.
        
        @param force: set to True to force creating a new hasher even if
            the hash has been verified already
        """
        if self.result is None or force == True:
            self.result = None
            self.size = 0
            self.done = False
            if sys.version_info < (2, 5):
                mod = __import__(self.ORDER[self.hashTypeNum]['old_module'], globals(), locals(), [])
                self.fileHasher = mod.new()
            else:
                import hashlib
                func = getattr(hashlib, self.ORDER[self.hashTypeNum]['hashlib_func'])
                self.fileHasher = func()

    def update(self, data):
        """Add more data to the file hasher."""
        if self.result is None:
            assert self.done == False, "Already done, you can't add more data after calling digest() or verify()"
            assert self.fileHasher is not None, "file hasher not initialized"
            self.fileHasher.update(data)
            self.size += len(data)
        
    def digest(self):
        """Get the hash of the added file data."""
        if self.fileHash is None:
            assert self.fileHasher is not None, "you must hash some data first"
            self.fileHash = self.fileHasher.digest()
            self.done = True
        return self.fileHash

    def hexdigest(self):
        """Get the hash of the added file data in hex format."""
        if self.fileHex is None:
            self.fileHex = b2a_hex(self.digest())
        return self.fileHex
        
    def norm(self, bits=None, bytes=None):
        """Normalize the binary hash for the given length.
        
        You must specify one of bits or bytes.
        """
        if self.fileNormHash is None:
            self.fileNormHash = self._norm_hash(self.digest(), bits, bytes)
        return self.fileNormHash

    def verify(self):
        """Verify that the added file data hash matches the expected hash."""
        if self.result is None and self.fileHash is not None and self.expHash is not None:
            self.result = (self.fileHash == self.expHash and self.size == self.expSize)
        return self.result
    
    #### Methods for setting the expected hash
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
    
    def test_normalize(self):
        h = HashObject()
        h.set(h.ORDER[0], b2a_hex('12345678901234567890'), '0')
        self.failUnless(h.normexpected(bits = 160) == '12345678901234567890')
        h = HashObject()
        h.set(h.ORDER[0], b2a_hex('12345678901234567'), '0')
        self.failUnless(h.normexpected(bits = 160) == '12345678901234567\000\000\000')
        h = HashObject()
        h.set(h.ORDER[0], b2a_hex('1234567890123456789012345'), '0')
        self.failUnless(h.normexpected(bytes = 20) == '12345678901234567890')
        h = HashObject()
        h.set(h.ORDER[0], b2a_hex('1234567890123456789'), '0')
        self.failUnless(h.normexpected(bytes = 20) == '1234567890123456789\000')
        h = HashObject()
        h.set(h.ORDER[0], b2a_hex('123456789012345678901'), '0')
        self.failUnless(h.normexpected(bits = 160) == '12345678901234567890')

    def test_failure(self):
        h = HashObject()
        h.set(h.ORDER[0], b2a_hex('12345678901234567890'), '0')
        self.failUnlessRaises(AssertionError, h.normexpected)
        self.failUnlessRaises(AssertionError, h.digest)
        self.failUnlessRaises(AssertionError, h.hexdigest)
        self.failUnlessRaises(AssertionError, h.update, 'gfgf')
    
    def test_sha1(self):
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
        self.failUnlessRaises(AssertionError, h.update, 'gfgf')
        self.failUnless(h.verify() == True)
        
    def test_md5(self):
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
        self.failUnlessRaises(AssertionError, h.update, 'gfgf')
        self.failUnless(h.verify() == True)
        
    def test_sha256(self):
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
        self.failUnlessRaises(AssertionError, h.update, 'gfgf')
        self.failUnless(h.verify() == True)

    if sys.version_info < (2, 5):
        test_sha256.skip = "SHA256 hashes are not supported by Python until version 2.5"
