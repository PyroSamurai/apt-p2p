## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

import khash
import time
from types import *

class Node:
    """encapsulate contact info"""
    def __init__(self):
        self.fails = 0
        self.lastSeen = 0
        self.id = self.host = self.port = ''
    
    def init(self, id, host, port):
        self.id = id
        self.num = khash.intify(id)
        self.host = host
        self.port = port
        self._senderDict = {'id': self.id, 'port' : self.port, 'host' : self.host}
        return self
    
    def initWithDict(self, dict):
        self._senderDict = dict
        self.id = dict['id']
        self.num = khash.intify(self.id)
        self.port = dict['port']
        self.host = dict['host']
        return self
    
    def updateLastSeen(self):
        self.lastSeen = time.time()
        self.fails = 0
    
    def msgFailed(self):
        self.fails = self.fails + 1
        return self.fails
    
    def senderDict(self):
        return self._senderDict
    
    def __repr__(self):
        return `(self.id, self.host, self.port)`
    
    ## these comparators let us bisect/index a list full of nodes with either a node or an int/long
    def __lt__(self, a):
        if type(a) == InstanceType:
            a = a.num
        return self.num < a
    def __le__(self, a):
        if type(a) == InstanceType:
            a = a.num
        return self.num <= a
    def __gt__(self, a):
        if type(a) == InstanceType:
            a = a.num
        return self.num > a
    def __ge__(self, a):
        if type(a) == InstanceType:
            a = a.num
        return self.num >= a
    def __eq__(self, a):
        if type(a) == InstanceType:
            a = a.num
        return self.num == a
    def __ne__(self, a):
        if type(a) == InstanceType:
            a = a.num
        return self.num != a


import unittest

class TestNode(unittest.TestCase):
    def setUp(self):
        self.node = Node().init(khash.newID(), 'localhost', 2002)
    def testUpdateLastSeen(self):
        t = self.node.lastSeen
        self.node.updateLastSeen()
        assert t < self.node.lastSeen
    