
from twisted.internet import defer
from twisted.web2 import server, http, http_headers
from twisted.python import log

from apt_dht_conf import config
from PeerManager import PeerManager
from HTTPServer import TopLevel
from MirrorManager import MirrorManager

class AptDHT:
    def __init__(self, dht):
        log.msg('Initializing the main apt_dht application')
        self.dht = dht
        self.http_server = TopLevel(config.get('DEFAULT', 'cache_dir'), self)
        self.http_site = server.Site(self.http_server)
        self.peers = PeerManager()
        self.mirrors = MirrorManager(config.get('DEFAULT', 'cache_dir'))
    
    def getSite(self):
        return self.http_site
    
    def check_freshness(self, path, modtime, resp):
        d = self.peers.get([path], "HEAD", modtime)
        d.addCallback(self.check_freshness_done, path, resp)
        return d
    
    def check_freshness_done(self, resp, path, orig_resp):
        if resp.code == "304":
            return orig_resp
        else:
            return self.get_resp(path)
    
    def get_resp(self, path):
        d = defer.Deferred()
        
        findDefer = self.mirrors.findHash(path)
        
        findDefer.addcallback(self.findHash_done, path, d)
        return d
        
    def findHash_done(self, (hash, size), path, d):
        if hash is None:
            getDefer = self.peers.get([path])
            getDefer.addCallback(d.callback)
        else:
            # Lookup hash from DHT
            lookupDefer = self.dht.getValue(hash)
            lookupDefer.addCallback(self.lookupHash_done, hash, size, path, d)
            
    def lookupHash_done(self, locations, hash, size, path, d):
        if not locations:
            getDefer = self.peers.get([path])
            getDefer.addCallback(d.callback)
        else:
            # Download from the found peers
            getDefer = self.peers.get(locations)
            getDefer.addCallback(d.callback)
            