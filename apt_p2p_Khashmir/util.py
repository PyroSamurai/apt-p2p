
"""Some utitlity functions for use in apt-p2p's khashmir DHT."""

from twisted.trial import unittest

from khash import HASH_LENGTH

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
    if (len(s) != HASH_LENGTH+6):
        raise ValueError
    id = s[:HASH_LENGTH]
    host = '.'.join([str(ord(i)) for i in s[HASH_LENGTH:(HASH_LENGTH+4)]])
    port = (ord(s[HASH_LENGTH+4]) << 8) | ord(s[HASH_LENGTH+5])
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

def byte_format(s):
    """Format a byte size for reading by the user.
    
    @type s: C{long}
    @param s: the number of bytes
    @rtype: C{string}
    @return: the formatted size with appropriate units
    """
    if (s < 1):
        r = str(int(s*1000.0)/1000.0) + 'B'
    elif (s < 10):
        r = str(int(s*100.0)/100.0) + 'B'
    elif (s < 102):
        r = str(int(s*10.0)/10.0) + 'B'
    elif (s < 1024):
        r = str(int(s)) + 'B'
    elif (s < 10485):
        r = str(int((s/1024.0)*100.0)/100.0) + 'KiB'
    elif (s < 104857):
        r = str(int((s/1024.0)*10.0)/10.0) + 'KiB'
    elif (s < 1048576):
        r = str(int(s/1024)) + 'KiB'
    elif (s < 10737418L):
        r = str(int((s/1048576.0)*100.0)/100.0) + 'MiB'
    elif (s < 107374182L):
        r = str(int((s/1048576.0)*10.0)/10.0) + 'MiB'
    elif (s < 1073741824L):
        r = str(int(s/1048576)) + 'MiB'
    elif (s < 1099511627776L):
        r = str(int((s/1073741824.0)*100.0)/100.0) + 'GiB'
    else:
        r = str(int((s/1099511627776.0)*100.0)/100.0) + 'TiB'
    return(r)

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
        