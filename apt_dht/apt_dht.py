
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
        
        findDefer.addCallback(self.findHash_done, path, d)
        findDefer.addErrback(self.findHash_error, path, d)
        return d
    
    def findHash_error(self, failure, path, d):
        log.err(failure)
        self.findHash_done((None, None), path, d)
        
    def findHash_done(self, (hash, size), path, d):
        if hash is None:
            log.msg('Hash for %s was not found' % path)
            self.download_file([path], hash, size, path, d)
        else:
            log.msg('Found hash %s for %s' % (hash, path))
            # Lookup hash from DHT
            lookupDefer = self.dht.getValue(hash)
            lookupDefer.addCallback(self.lookupHash_done, hash, size, path, d)
            
    def lookupHash_done(self, locations, hash, size, path, d):
        if not locations:
            log.msg('Peers for %s were not found' % path)
            self.download_file([path], hash, size, path, d)
        else:
            log.msg('Found peers for $s: %r' % (path, locations))
            # Download from the found peers
            self.download_file(locations, hash, size, path, d)
            
    def download_file(self, locations, hash, size, path, d):
        getDefer = self.peers.get(locations)
        getDefer.addCallback(self.mirrors.save_file, hash, size, path)
        getDefer.addErrback(self.mirrors.save_error, path)
        getDefer.addCallbacks(d.callback, d.errback)
