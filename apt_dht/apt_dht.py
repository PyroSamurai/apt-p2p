
from binascii import b2a_hex
import os.path

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
        self.http_server = TopLevel(config.get('DEFAULT', 'cache_dir'), self)
        self.http_site = server.Site(self.http_server)
        self.peers = PeerManager()
        self.mirrors = MirrorManager(self, config.get('DEFAULT', 'cache_dir'))
    
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
