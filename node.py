import hash
import time
from types import *
from xmlrpclib import Binary

class Node:
    """encapsulate contact info"""
    def init(self, id, host, port):
	self.id = id
	self.int = hash.intify(id)
	self.host = host
	self.port = port
	self.lastSeen = time.time()
	self._senderDict = {'id': Binary(self.id), 'port' : self.port, 'host' : self.host}
	return self
	
    def initWithDict(self, dict):
	self._senderDict = dict
	self.id = dict['id'].data
	self.int = hash.intify(self.id)
	self.port = dict['port']
	self.host = dict['host']
	self.lastSeen = time.time()
	return self
	
    def updateLastSeen(self):
	self.lastSeen = time.time()

    def senderDict(self):
	return self._senderDict
	
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
	self.node = Node().init(hash.newID(), 'localhost', 2002)
    def testUpdateLastSeen(self):
	t = self.node.lastSeen
	self.node.updateLastSeen()
	assert t < self.node.lastSeen
	