## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

"""Represents a node in the DHT.

@type NULL_ID: C{string}
@var NULL_ID: the node ID to use until one is known
"""

from datetime import datetime, MINYEAR
from types import InstanceType

from twisted.trial import unittest

import khash
from util import compact

# magic id to use before we know a peer's id
NULL_ID = 20 * '\0'

class Node:
    """Encapsulate a node's contact info.
    
    @type fails: C{int}
    @ivar fails: number of times this node has failed in a row
    @type lastSeen: C{datetime.datetime}
    @ivar lastSeen: the last time a response was received from this node
    @type id: C{string}
    @ivar id: the node's ID in the DHT
    @type num: C{long}
    @ivar num: the node's ID in number form
    @type host: C{string}
    @ivar host: the IP address of the node
    @type port: C{int}
    @ivar port: the port of the node
    @type token: C{string}
    @ivar token: the last received token from the node
    @type num_values: C{int}
    @ivar num_values: the number of values the node has for the key in the
        currently executing action
    """
    
    def __init__(self, id, host = None, port = None):
        """Initialize the node.
        
        @type id: C{string} or C{dictionary}
        @param id: the node's ID in the DHT, or a dictionary containing the
            node's id, host and port
        @type host: C{string}
        @param host: the IP address of the node
            (optional, but must be specified if id is not a dictionary)
        @type port: C{int}
        @param port: the port of the node
            (optional, but must be specified if id is not a dictionary)
        """
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
        self.token = ''
        self.num_values = 0
        self._contactInfo = None
    
    def updateLastSeen(self):
        """Updates the last contact time of the node and resets the number of failures."""
        self.lastSeen = datetime.now()
        self.fails = 0
        
    def updateToken(self, token):
        """Update the token for the node."""
        self.token = token
    
    def updateNumValues(self, num_values):
        """Update how many values the node has in the current search for a value."""
        self.num_values = num_values
    
    def msgFailed(self):
        """Log a failed attempt to contact this node.
        
        @rtype: C{int}
        @return: the number of consecutive failures this node has
        """
        self.fails = self.fails + 1
        return self.fails
    
    def contactInfo(self):
        """Get the compact contact info for the node."""
        if self._contactInfo is None:
            self._contactInfo = compact(self.id, self.host, self.port)
        return self._contactInfo
    
    def __repr__(self):
        return `(self.id, self.host, self.port)`
    
    #{ Comparators to bisect/index a list of nodes with either a node or a long
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
    """Unit tests for the node implementation."""
    def setUp(self):
        self.node = Node(khash.newID(), '127.0.0.1', 2002)
    def testUpdateLastSeen(self):
        t = self.node.lastSeen
        self.node.updateLastSeen()
        self.failUnless(t < self.node.lastSeen)
    