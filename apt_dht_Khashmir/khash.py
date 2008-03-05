## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

"""Functions to deal with hashes (node IDs and keys)."""

from sha import sha
from os import urandom

from twisted.trial import unittest

def intify(hstr):
    """Convert a hash (big-endian) to a long python integer."""
    assert len(hstr) == 20
    return long(hstr.encode('hex'), 16)

def stringify(num):
    """Convert a long python integer to a hash."""
    str = hex(num)[2:]
    if str[-1] == 'L':
        str = str[:-1]
    if len(str) % 2 != 0:
        str = '0' + str
    str = str.decode('hex')
    return (20 - len(str)) *'\x00' + str
    
def distance(a, b):
    """Calculate the distance between two hashes expressed as strings."""
    return intify(a) ^ intify(b)

def newID():
    """Get a new pseudorandom globally unique hash string."""
    h = sha()
    h.update(urandom(20))
    return h.digest()

def newIDInRange(min, max):
    """Get a new pseudorandom globally unique hash string in the range."""
    return stringify(randRange(min,max))
    
def randRange(min, max):
    """Get a new pseudorandom globally unique hash number in the range."""
    return min + intify(newID()) % (max - min)
    
def newTID():
    """Get a new pseudorandom transaction ID number."""
    return randRange(-2**30, 2**30)

class TestNewID(unittest.TestCase):
    """Test the newID function."""
    def testLength(self):
        self.failUnlessEqual(len(newID()), 20)
    def testHundreds(self):
        for x in xrange(100):
            self.testLength

class TestIntify(unittest.TestCase):
    """Test the intify function."""
    known = [('\0' * 20, 0),
            ('\xff' * 20, 2L**160 - 1),
            ]
    def testKnown(self):
        for str, value in self.known: 
            self.failUnlessEqual(intify(str),  value)
    def testEndianessOnce(self):
        h = newID()
        while h[-1] == '\xff':
            h = newID()
        k = h[:-1] + chr(ord(h[-1]) + 1)
        self.failUnlessEqual(intify(k) - intify(h), 1)
    def testEndianessLots(self):
        for x in xrange(100):
            self.testEndianessOnce()

class TestDisantance(unittest.TestCase):
    """Test the distance function."""
    known = [
            (("\0" * 20, "\xff" * 20), 2**160L -1),
            ((sha("foo").digest(), sha("foo").digest()), 0),
            ((sha("bar").digest(), sha("bar").digest()), 0)
            ]
    def testKnown(self):
        for pair, dist in self.known:
            self.failUnlessEqual(distance(pair[0], pair[1]), dist)
    def testCommutitive(self):
        for i in xrange(100):
            x, y, z = newID(), newID(), newID()
            self.failUnlessEqual(distance(x,y) ^ distance(y, z), distance(x, z))
        
class TestRandRange(unittest.TestCase):
    """Test the randRange function."""
    def testOnce(self):
        a = intify(newID())
        b = intify(newID())
        if a < b:
            c = randRange(a, b)
            self.failUnlessEqual(a <= c < b, True, "output out of range %d  %d  %d" % (b, c, a))
        else:
            c = randRange(b, a)
            self.failUnlessEqual(b <= c < a, True, "output out of range %d  %d  %d" % (b, c, a))

    def testOneHundredTimes(self):
        for i in xrange(100):
            self.testOnce()
