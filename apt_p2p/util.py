
"""Some utitlity functions for use in the apt-p2p program.

@var isLocal: a compiled regular expression suitable for testing if an
    IP address is from a known local or private range
"""

import os, re

from twisted.python import log
from twisted.trial import unittest

isLocal = re.compile('^(192\.168\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(172\.0?([1][6-9])|([2][0-9])|([3][0-1])\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(127\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})$')

def findMyIPAddr(addrs, intended_port, local_ok = False):
    """Find the best IP address to use from a list of possibilities.
    
    @param addrs: the list of possible IP addresses
    @param intended_port: the port that was supposed to be used
    @param local_ok: whether known local/private IP ranges are allowed
        (defaults to False)
    @return: the preferred IP address, or None if one couldn't be found
    """
    log.msg("got addrs: %r" % (addrs,))
    my_addr = None
    
    # Try to find an address using the ifconfig function
    try:
        ifconfig = os.popen("/sbin/ifconfig |/bin/grep inet|"+
                            "/usr/bin/awk '{print $2}' | "+
                            "sed -e s/.*://", "r").read().strip().split('\n')
    except:
        ifconfig = []

    # Get counts for all the non-local addresses returned from ifconfig
    addr_count = {}
    for addr in ifconfig:
        if local_ok or not isLocal.match(addr):
            addr_count.setdefault(addr, 0)
            addr_count[addr] += 1
    
    # If only one was found, use it as a starting point
    local_addrs = addr_count.keys()    
    if len(local_addrs) == 1:
        my_addr = local_addrs[0]
        log.msg('Found remote address from ifconfig: %r' % (my_addr,))
    
    # Get counts for all the non-local addresses returned from the DHT
    addr_count = {}
    port_count = {}
    for addr in addrs:
        if local_ok or not isLocal.match(addr[0]):
            addr_count.setdefault(addr[0], 0)
            addr_count[addr[0]] += 1
            port_count.setdefault(addr[1], 0)
            port_count[addr[1]] += 1
    
    # Find the most popular address
    popular_addr = []
    popular_count = 0
    for addr in addr_count:
        if addr_count[addr] > popular_count:
            popular_addr = [addr]
            popular_count = addr_count[addr]
        elif addr_count[addr] == popular_count:
            popular_addr.append(addr)
    
    # Find the most popular port
    popular_port = []
    popular_count = 0
    for port in port_count:
        if port_count[port] > popular_count:
            popular_port = [port]
            popular_count = port_count[port]
        elif port_count[port] == popular_count:
            popular_port.append(port)

    # Check to make sure the port isn't being changed
    port = intended_port
    if len(port_count.keys()) > 1:
        log.msg('Problem, multiple ports have been found: %r' % (port_count,))
        if port not in port_count.keys():
            log.msg('And none of the ports found match the intended one')
    elif len(port_count.keys()) == 1:
        port = port_count.keys()[0]
    else:
        log.msg('Port was not found')

    # If one is popular, use that address
    if len(popular_addr) == 1:
        log.msg('Found popular address: %r' % (popular_addr[0],))
        if my_addr and my_addr != popular_addr[0]:
            log.msg('But the popular address does not match: %s != %s' % (popular_addr[0], my_addr))
        my_addr = popular_addr[0]
    elif len(popular_addr) > 1:
        log.msg('Found multiple popular addresses: %r' % (popular_addr,))
        if my_addr and my_addr not in popular_addr:
            log.msg('And none of the addresses found match the ifconfig one')
    else:
        log.msg('No non-local addresses found: %r' % (popular_addr,))
        
    if not my_addr:
        log.msg("Remote IP Address could not be found for this machine")
        
    return my_addr

def ipAddrFromChicken():
    """Retrieve a possible IP address from the ipchecken website."""
    import urllib
    ip_search = re.compile('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')
    try:
         f = urllib.urlopen("http://www.ipchicken.com")
         data = f.read()
         f.close()
         current_ip = ip_search.findall(data)
         return current_ip
    except Exception:
         return []

def uncompact(s):
    """Extract the contact info from a compact peer representation.
    
    @type s: C{string}
    @param s: the compact representation
    @rtype: (C{string}, C{int})
    @return: the IP address and port number to contact the peer on
    @raise ValueError: if the compact representation doesn't exist
    """
    if (len(s) != 6):
        raise ValueError
    ip = '.'.join([str(ord(i)) for i in s[0:4]])
    port = (ord(s[4]) << 8) | ord(s[5])
    return (ip, port)

def compact(ip, port):
    """Create a compact representation of peer contact info.
    
    @type ip: C{string}
    @param ip: the IP address of the peer
    @type port: C{int}
    @param port: the port number to contact the peer on
    @rtype: C{string}
    @return: the compact representation
    @raise ValueError: if the compact representation doesn't exist
    """
    
    s = ''.join([chr(int(i)) for i in ip.split('.')]) + \
          chr((port & 0xFF00) >> 8) + chr(port & 0xFF)
    if len(s) != 6:
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
    ip = '165.234.1.34'
    port = 61234

    def test_compact(self):
        """Make sure compacting is reversed correctly by uncompacting."""
        d = uncompact(compact(self.ip, self.port))
        self.failUnlessEqual(d[0], self.ip)
        self.failUnlessEqual(d[1], self.port)
