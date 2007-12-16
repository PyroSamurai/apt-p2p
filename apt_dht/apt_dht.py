
from twisted.internet import defer
from twisted.web2 import server, http, http_headers

from apt_dht_conf import config
from PeerManager import PeerManager
from HTTPServer import TopLevel
from MirrorManager import MirrorManager

class AptDHT:
    def __init__(self):
        self.http_server = TopLevel(config.defaults()['cache_dir'], self)
        self.http_site = server.Site(self.http_server)
        self.peers = PeerManager()
        self.mirrors = MirrorManager(config.defaults()['cache_dir'])
    
    def getSite(self):
        return self.http_site
    
    def check_freshness(self, path, modtime, resp):
        host, path = path.split('/',1)
        if not host:
            host, path = path.split('/',1)
        path = '/'+path
        
        # Make sure a port is included for consistency
        if host.find(':') >= 0:
            host, port = host.split(':', 1)
            port = int(port)
        else:
            port = 80
        
        d = self.peers.get([(host, port, path)], "HEAD", modtime)
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
            host, path = path.split('/',1)
            if not host:
                host, path = path.split('/',1)
            path = '/'+path
            
            # Make sure a port is included for consistency
            if host.find(':') >= 0:
                host, port = host.split(':', 1)
                port = int(port)
            else:
                port = 80
            getDefer = self.peers.get([(host, port, path)])
            getDefer.addCallback(d.callback)
            