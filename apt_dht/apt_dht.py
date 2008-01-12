
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
from db import DB
from util import findMyIPAddr

class AptDHT:
    def __init__(self, dht):
        log.msg('Initializing the main apt_dht application')
        self.db = DB(config.get('DEFAULT', 'cache_dir') + '/.apt-dht.db')
        self.dht = dht
        self.dht.loadConfig(config, config.get('DEFAULT', 'DHT'))
        self.dht.join().addCallbacks(self.joinComplete, self.joinError)
        self.http_server = TopLevel(config.get('DEFAULT', 'cache_dir'), self)
        self.http_site = server.Site(self.http_server)
        self.peers = PeerManager()
        self.mirrors = MirrorManager(config.get('DEFAULT', 'cache_dir'), self)
        self.my_addr = None
    
    def getSite(self):
        return self.http_site
    
    def joinComplete(self, result):
        self.my_addr = findMyIPAddr(result, config.getint(config.get('DEFAULT', 'DHT'), 'PORT'))

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
            self.lookupHash_done([], hash, path, d)
        else:
            log.msg('Found hash %s for %s' % (hash.hexexpected(), path))
            # Lookup hash from DHT
            key = hash.normexpected(bits = config.getint(config.get('DEFAULT', 'DHT'), 'HASH_LENGTH'))
            lookupDefer = self.dht.getValue(key)
            lookupDefer.addCallback(self.lookupHash_done, hash, path, d)
            
    def lookupHash_done(self, locations, hash, path, d):
        if not locations:
            log.msg('Peers for %s were not found' % path)
            getDefer = self.peers.get([path])
            getDefer.addCallback(self.mirrors.save_file, hash, path)
            getDefer.addErrback(self.mirrors.save_error, path)
            getDefer.addCallbacks(d.callback, d.errback)
        else:
            log.msg('Found peers for %s: %r' % (path, locations))
            # Download from the found peers
            getDefer = self.peers.get(locations)
            getDefer.addCallback(self.check_response, hash, path)
            getDefer.addCallback(self.mirrors.save_file, hash, path)
            getDefer.addErrback(self.mirrors.save_error, path)
            getDefer.addCallbacks(d.callback, d.errback)
            
    def check_response(self, response, hash, path):
        if response.code < 200 or response.code >= 300:
            log.msg('Download from peers failed, going to direct download: %s' % path)
            getDefer = self.peers.get([path])
            return getDefer
        return response
        
    def cached_file(self, hash, url, file_path):
        assert file_path.startswith(config.get('DEFAULT', 'cache_dir'))
        urlpath, newdir = self.db.storeFile(file_path, hash.digest(), config.get('DEFAULT', 'cache_dir'))
        log.msg('now avaliable at %s: %s' % (urlpath, url))

        if self.my_addr:
            site = self.my_addr + ':' + str(config.getint('DEFAULT', 'PORT'))
            full_path = urlunparse(('http', site, urlpath, None, None, None))
            key = hash.norm(bits = config.getint(config.get('DEFAULT', 'DHT'), 'HASH_LENGTH'))
            storeDefer = self.dht.storeValue(key, full_path)
            storeDefer.addCallback(self.store_done, full_path)
            storeDefer.addErrback(log.err)

    def store_done(self, result, path):
        log.msg('Added %s to the DHT: %r' % (path, result))
        