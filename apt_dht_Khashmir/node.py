## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

from datetime import datetime, MINYEAR
from types import InstanceType

from twisted.trial import unittest

import khash
from util import compact

# magic id to use before we know a peer's id
NULL_ID = 20 * '\0'

class Node:
    """encapsulate contact info"""
    def __init__(self, id, host = None, port = None):
        self.fails = 0
        self.lastSeen = datetime(MINYEAR, 1, 1)

        # Alternate method, init Node from dictionary
        if isinstance(id, dict):
            host = id['host']
            port = id['port']
            id = id['id']

        assert isinstance(id, str)
        assert isinstance(host, str)
        self.id = id
        self.num = khash.intify(id)
        self.host = host
        self.port = int(port)
        self._contactInfo = None
    
    def updateLastSeen(self):
        self.lastSeen = datetime.now()
        self.fails = 0
    
    def msgFailed(self):
        self.fails = self.fails + 1
        return self.fails
    
    def contactInfo(self):
        if self._contactInfo is None:
            self._contactInfo = compact(self.id, self.host, self.port)
        return self._contactInfo
    
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


class TestNode(unittest.TestCase):
    def setUp(self):
        self.node = Node(khash.newID(), '127.0.0.1', 2002)
    def testUpdateLastSeen(self):
        t = self.node.lastSeen
        self.node.updateLastSeen()
        self.failUnless(t < self.node.lastSeen)
    