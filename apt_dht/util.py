## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

import os, re

from twisted.python import log

isLocal = re.compile('^(192\.168\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(172\.0?([1][6-9])|([2][0-9])|([3][0-1])\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(127\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})$')

def findMyIPAddr(addrs, intended_port):
    log.msg("got addrs: %r" % (addrs,))
    my_addr = None
    
    try:
        ifconfig = os.popen("/sbin/ifconfig |/bin/grep inet|"+
                            "/usr/bin/awk '{print $2}' | "+
                            "sed -e s/.*://", "r").read().strip().split('\n')
    except:
        ifconfig = []

    # Get counts for all the non-local addresses returned
    addr_count = {}
    for addr in ifconfig:
        if not isLocal.match(addr):
            addr_count.setdefault(addr, 0)
            addr_count[addr] += 1
    
    local_addrs = addr_count.keys()    
    if len(local_addrs) == 1:
        my_addr = local_addrs[0]
        log.msg('Found remote address from ifconfig: %r' % (my_addr,))
    
    # Get counts for all the non-local addresses returned
    addr_count = {}
    port_count = {}
    for addr in addrs:
        if not isLocal.match(addr[0]):
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
            
    port = intended_port
    if len(port_count.keys()) > 1:
        log.msg('Problem, multiple ports have been found: %r' % (port_count,))
        if port not in port_count.keys():
            log.msg('And none of the ports found match the intended one')
    elif len(port_count.keys()) == 1:
        port = port_count.keys()[0]
    else:
        log.msg('Port was not found')

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
