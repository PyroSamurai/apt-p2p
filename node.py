import hash
import time
from types import *

class Node:
    """encapsulate contact info"""
    def __init__(self, id, host, port):
	self.id = id
	self.int = hash.intify(id)
	self.host = host
	self.port = port
	self.lastSeen = time.time()
	
    def updateLastSeen(self):
	self.lastSeen = time.time()

    def senderDict(self):
	return {'id': self.id, 'port' : self.port, 'host' : self.host}
	
    def __repr__(self):
	return `(self.id, self.host, self.port)`
	
    ## these comparators let us bisect/index a list full of nodes with either a node or an int/long
    def __lt__(self, a):
	if type(a) == InstanceType:
	    a = a.int
	return self.int < a
    def __le__(self, a):
    	if type(a) == InstanceType:
	    a = a.int
	return self.int <= a
    def __gt__(self, a):
    	if type(a) == InstanceType:
	    a = a.int
	return self.int > a
    def __ge__(self, a):
    	if type(a) == InstanceType:
	    a = a.int
	return self.int >= a
    def __eq__(self, a):
    	if type(a) == InstanceType:
	    a = a.int
	return self.int == a
    def __ne__(self, a):
    	if type(a) == InstanceType:
	    a = a.int
	return self.int != a


import unittest

class TestNode(unittest.TestCase):
    def setUp(self):
	self.node = Node(hash.newID(), 'localhost', 2002)
    def testUpdateLastSeen(self):
	t = self.node.lastSeen
	self.node.updateLastSeen()
	assert t < self.node.lastSeen
	