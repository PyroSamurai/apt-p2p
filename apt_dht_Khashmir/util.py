## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

"""Some utitlity functions for use in apt-p2p's khashmir DHT."""

from twisted.trial import unittest

def bucket_stats(l):
    """Given a list of khashmir instances, finds min, max, and average number of nodes in tables."""
    max = avg = 0
    min = None
    def count(buckets):
        c = 0
        for bucket in buckets:
            c = c + len(bucket.l)
        return c
    for node in l:
        c = count(node.table.buckets)
        if min == None:
            min = c
        elif c < min:
            min = c
        if c > max:
            max = c
        avg = avg + c
    avg = avg / len(l)
    return {'min':min, 'max':max, 'avg':avg}

def uncompact(s):
    """Extract the contact info from a compact node representation.
    
    @type s: C{string}
    @param s: the compact representation
    @rtype: C{dictionary}
    @return: the node ID, IP address and port to contact the node on
    @raise ValueError: if the compact representation doesn't exist
    """
    if (len(s) != 26):
        raise ValueError
    id = s[:20]
    host = '.'.join([str(ord(i)) for i in s[20:24]])
    port = (ord(s[24]) << 8) | ord(s[25])
    return {'id': id, 'host': host, 'port': port}

def compact(id, host, port):
    """Create a compact representation of node contact info.
    
    @type id: C{string}
    @param id: the node ID
    @type host: C{string}
    @param host: the IP address of the node
    @type port: C{int}
    @param port: the port number to contact the node on
    @rtype: C{string}
    @return: the compact representation
    @raise ValueError: if the compact representation doesn't exist
    """
    
    s = id + ''.join([chr(int(i)) for i in host.split('.')]) + \
          chr((port & 0xFF00) >> 8) + chr(port & 0xFF)
    if len(s) != 26:
        raise ValueError
    return s

class TestUtil(unittest.TestCase):
    """Tests for the utilities."""
    
    timeout = 5
    myid = '\xca\xec\xb8\x0c\x00\xe7\x07\xf8~])\x8f\x9d\xe5_B\xff\x1a\xc4!'
    host = '165.234.1.34'
    port = 61234

    def test_compact(self):
        d = uncompact(compact(self.myid, self.host, self.port))
        self.failUnlessEqual(d['id'], self.myid)
        self.failUnlessEqual(d['host'], self.host)
        self.failUnlessEqual(d['port'], self.port)
        