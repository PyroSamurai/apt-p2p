## Copyright 2002 Andrew Loewenstern, All Rights Reserved

from sha import sha
from whrandom import randrange

## takes a 20 bit hash, big-endian, and returns it expressed a python integer
## ha ha ha ha    if this were a C module I wouldn't resort to such sillyness
def intify(hstr):
    assert(len(hstr) == 20)
    i = 0L
    i = i + ord(hstr[19])
    i = i + ord(hstr[18]) * 256L
    i = i + ord(hstr[17]) * 65536L
    i = i + ord(hstr[16]) * 16777216L
    i = i + ord(hstr[15]) * 4294967296L
    i = i + ord(hstr[14]) * 1099511627776L
    i = i + ord(hstr[13]) * 281474976710656L
    i = i + ord(hstr[12]) * 72057594037927936L
    i = i + ord(hstr[11]) * 18446744073709551616L
    i = i + ord(hstr[10]) * 4722366482869645213696L
    i = i + ord(hstr[9]) * 1208925819614629174706176L
    i = i + ord(hstr[8]) * 309485009821345068724781056L
    i = i + ord(hstr[7]) * 79228162514264337593543950336L
    i = i + ord(hstr[6]) * 20282409603651670423947251286016L
    i = i + ord(hstr[5]) * 5192296858534827628530496329220096L
    i = i + ord(hstr[4]) * 1329227995784915872903807060280344576L
    i = i + ord(hstr[3]) * 340282366920938463463374607431768211456L
    i = i + ord(hstr[2]) * 87112285931760246646623899502532662132736L
    i = i + ord(hstr[1]) * 22300745198530623141535718272648361505980416L
    i = i + ord(hstr[0]) * 5708990770823839524233143877797980545530986496L
    return i

## returns the distance between two 160-bit hashes expressed as 20-character strings
def distance(a, b):
    return intify(a) ^ intify(b)


## returns a new pseudorandom globally unique ID string
def newID():
    h = sha()
    for i in range(20):
	h.update(chr(randrange(0,256)))
    return h.digest()

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
	     ('\xff' * 20, 2**160 - 1),
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
	    (("\0" * 20, "\xff" * 20), 2**160 -1),
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

    