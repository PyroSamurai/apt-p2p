## Copyright 2002 Andrew Loewenstern, All Rights Reserved

from sha import sha
from whrandom import randrange

## takes a 20 bit hash, big-endian, and returns it expressed a long python integer
def intify(hstr):
    assert(len(hstr) == 20)
    return eval('0x' + hstr.encode('hex') + 'L')

## takes a long int and returns a 20-character string
def stringify(int):
    str = hex(int)[2:]
    if str[-1] == 'L':
	str = str[:-1]
    if len(str) % 2 != 0:
	str = '0' + str
    str = str.decode('hex')
    return (20 - len(str)) *'\x00' + str
    
## returns the distance between two 160-bit hashes expressed as 20-character strings
def distance(a, b):
    return intify(a) ^ intify(b)


## returns a new pseudorandom globally unique ID string
def newID():
    h = sha()
    for i in range(20):
	h.update(chr(randrange(0,256)))
    return h.digest()

def newIDInRange(min, max):
    return stringify(randRange(min,max))
    
def randRange(min, max):
    return min + intify(newID()) % (max - min)

import unittest

class NewID(unittest.TestCase):
    def testLength(self):
	self.assertEqual(len(newID()), 20)
    def testHundreds(self):
	for x in xrange(100):
	    self.testLength

class Intify(unittest.TestCase):
    known = [('\0' * 20, 0),
	     ('\xff' * 20, 2L**160 - 1),
	    ]
    def testKnown(self):
	for str, value in self.known: 
	    self.assertEqual(intify(str),  value)
    def testEndianessOnce(self):
	h = newID()
	while h[-1] == '\xff':
	    h = newID()
	k = h[:-1] + chr(ord(h[-1]) + 1)
	self.assertEqual(intify(k) - intify(h), 1)
    def testEndianessLots(self):
	for x in xrange(100):
	    self.testEndianessOnce()

class Disantance(unittest.TestCase):
    known = [
	    (("\0" * 20, "\xff" * 20), 2**160L -1),
	    ((sha("foo").digest(), sha("foo").digest()), 0),
	    ((sha("bar").digest(), sha("bar").digest()), 0)
	    ]
    def testKnown(self):
	for pair, dist in self.known:
	    self.assertEqual(distance(pair[0], pair[1]), dist)
    def testCommutitive(self):
	for i in xrange(100):
	    x, y, z = newID(), newID(), newID()
	    self.assertEqual(distance(x,y) ^ distance(y, z), distance(x, z))
	
class RandRange(unittest.TestCase):
    def testOnce(self):
	a = intify(newID())
	b = intify(newID())
	if a < b:
	    c = randRange(a, b)
	    self.assertEqual(a <= c < b, 1, "output out of range %d  %d  %d" % (b, c, a))
	else:
	    c = randRange(b, a)
	    assert b <= c < a, "output out of range %d  %d  %d" % (b, c, a)

    def testOneHundredTimes(self):
	for i in xrange(100):
	    self.testOnce()



if __name__ == '__main__':
    unittest.main()   

    