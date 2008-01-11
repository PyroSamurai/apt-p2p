
from binascii import b2a_hex
from urlparse import urlunparse
import os, re

from twisted.internet import defer
from twisted.web2 import server, http, http_headers
from twisted.python import log

from apt_dht_conf import config
from PeerManager import PeerManager
from HTTPServer import TopLevel
from MirrorManager import MirrorManager
from Hash import HashObject

class AptDHT:
    def __init__(self, dht):
        log.msg('Initializing the main apt_dht application')
        self.dht = dht
        self.dht.loadConfig(config, config.get('DEFAULT', 'DHT'))
        self.dht.join().addCallbacks(self.joinComplete, self.joinError)
        self.http_server = TopLevel(config.get('DEFAULT', 'cache_dir'), self)
        self.http_site = server.Site(self.http_server)
        self.peers = PeerManager()
        self.mirrors = MirrorManager(config.get('DEFAULT', 'cache_dir'), self)
        self.my_addr = None
        self.isLocal = re.compile('^(192\.168\.[0-9]{1,3}\.[0-9]{1,3})|'+
                                  '(10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})|'+
                                  '(172\.0?([1][6-9])|([2][0-9])|([3][0-1])\.[0-9]{1,3}\.[0-9]{1,3})|'+
                                  '(127\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})$')
    
    def getSite(self):
        return self.http_site

    def joinComplete(self, addrs):
        log.msg("got addrs: %r" % (addrs,))
        
        try:
            ifconfig = os.popen("/sbin/ifconfig |/bin/grep inet|"+
                                "/usr/bin/awk '{print $2}' | "+
                                "sed -e s/.*://", "r").read().strip().split('\n')
        except:
            ifconfig = []

        # Get counts for all the non-local addresses returned
        addr_count = {}
        for addr in ifconfig:
            if not self.isLocal.match(addr):
                addr_count.setdefault(addr, 0)
                addr_count[addr] += 1
        
        local_addrs = addr_count.keys()    
        if len(local_addrs) == 1:
            self.my_addr = local_addrs[0]
            log.msg('Found remote address from ifconfig: %r' % (self.my_addr,))
        
        # Get counts for all the non-local addresses returned
        addr_count = {}
        port_count = {}
        for addr in addrs:
            if not self.isLocal.match(addr[0]):
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
                
        port = config.getint(config.get('DEFAULT', 'DHT'), 'PORT')
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
            if self.my_addr and self.my_addr != popular_addr[0]:
                log.msg('But the popular address does not match: %s != %s' % (popular_addr[0], self.my_addr))
            self.my_addr = popular_addr[0]
        elif len(popular_addr) > 1:
            log.msg('Found multiple popular addresses: %r' % (popular_addr,))
            if self.my_addr and self.my_addr not in popular_addr:
                log.msg('And none of the addresses found match the ifconfig one')
        else:
            log.msg('No non-local addresses found: %r' % (popular_addr,))
            
        if not self.my_addr:
            log.err(RuntimeError("Remote IP Address could not be found for this machine"))

    def ipAddrFromChicken(self):
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

    def joinError(self, failure):
        log.msg("joining DHT failed miserably")
        log.err(failure)
    
    def check_freshness(self, path, modtime, resp):
        log.msg('Checking if %s is still fresh' % path)
        d = self.peers.get([path], "HEAD", modtime)
        d.addCallback(self.check_freshness_done, path, resp)
        return d
    
    def check_freshness_done(self, resp, path, orig_resp):
        if resp.code == 304:
            log.msg('Still fresh, returning: %s' % path)
            return orig_resp
        else:
            log.msg('Stale, need to redownload: %s' % path)
            return self.get_resp(path)
    
    def get_resp(self, path):
        d = defer.Deferred()
        
        log.msg('Trying to find hash for %s' % path)
        findDefer = self.mirrors.findHash(path)
        
        findDefer.addCallbacks(self.findHash_done, self.findHash_error, 
                               callbackArgs=(path, d), errbackArgs=(path, d))
        findDefer.addErrback(log.err)
        return d
    
    def findHash_error(self, failure, path, d):
        log.err(failure)
        self.findHash_done(HashObject(), path, d)
        
    def findHash_done(self, hash, path, d):
        if hash.expected() is None:
            log.msg('Hash for %s was not found' % path)
            self.download_file([path], hash, path, d)
        else:
            log.msg('Found hash %s for %s' % (hash.hexexpected(), path))
            # Lookup hash from DHT
            key = hash.normexpected(bits = config.getint(config.get('DEFAULT', 'DHT'), 'HASH_LENGTH'))
            lookupDefer = self.dht.getValue(key)
            lookupDefer.addCallback(self.lookupHash_done, hash, path, d)
            
    def lookupHash_done(self, locations, hash, path, d):
        if not locations:
            log.msg('Peers for %s were not found' % path)
            self.download_file([path], hash, path, d)
        else:
            log.msg('Found peers for %s: %r' % (path, locations))
            # Download from the found peers
            self.download_file(locations, hash, path, d)
            
    def download_file(self, locations, hash, path, d):
        getDefer = self.peers.get(locations)
        getDefer.addCallback(self.mirrors.save_file, hash, path)
        getDefer.addErrback(self.mirrors.save_error, path)
        getDefer.addCallbacks(d.callback, d.errback)
        
    def download_complete(self, hash, url, file_path):
        assert file_path.startswith(config.get('DEFAULT', 'cache_dir'))
        directory = file_path[:len(config.get('DEFAULT', 'cache_dir'))]
        url_path = file_path[len(config.get('DEFAULT', 'cache_dir')):]
        if url_path[0] == '/':
            url_path = url_path[1:]
        top_directory = url_path.split('/',1)[0]
        url_path = url_path[len(top_directory):]
        http_dir = os.path.join(directory, top_directory)
        new_top = self.http_server.addDirectory(http_dir)
        url_path = '/' + new_top + url_path
        log.msg('now avaliable at %s: %s' % (url_path, url))

        if self.my_addr:
            site = self.my_addr + ':' + str(config.getint('DEFAULT', 'PORT'))
            full_path = urlunparse(('http', site, url_path, None, None, None))
            key = hash.norm(bits = config.getint(config.get('DEFAULT', 'DHT'), 'HASH_LENGTH'))
            storeDefer = self.dht.storeValue(key, full_path)
            storeDefer.addCallback(self.store_done, full_path)
            storeDefer.addErrback(log.err)

    def store_done(self, result, path):
        log.msg('Added %s to the DHT: %r' % (path, result))
        