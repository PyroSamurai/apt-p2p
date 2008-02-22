
from binascii import b2a_hex
from urlparse import urlunparse
import os, re

from twisted.internet import defer
from twisted.web2 import server, http, http_headers, static
from twisted.python import log, failure
from twisted.python.filepath import FilePath

from apt_dht_conf import config
from PeerManager import PeerManager
from HTTPServer import TopLevel
from MirrorManager import MirrorManager
from CacheManager import CacheManager
from Hash import HashObject
from db import DB
from util import findMyIPAddr, compact

download_dir = 'cache'

class AptDHT:
    def __init__(self, dht):
        log.msg('Initializing the main apt_dht application')
        self.cache_dir = FilePath(config.get('DEFAULT', 'cache_dir'))
        if not self.cache_dir.child(download_dir).exists():
            self.cache_dir.child(download_dir).makedirs()
        self.db = DB(self.cache_dir.child('apt-dht.db'))
        self.dht = dht
        self.dht.loadConfig(config, config.get('DEFAULT', 'DHT'))
        self.dht.join().addCallbacks(self.joinComplete, self.joinError)
        self.http_server = TopLevel(self.cache_dir.child(download_dir), self.db, self)
        self.getHTTPFactory = self.http_server.getHTTPFactory
        self.peers = PeerManager()
        self.mirrors = MirrorManager(self.cache_dir, config.gettime('DEFAULT', 'UNLOAD_PACKAGES_CACHE'))
        other_dirs = [FilePath(f) for f in config.getstringlist('DEFAULT', 'OTHER_DIRS')]
        self.cache = CacheManager(self.cache_dir.child(download_dir), self.db, other_dirs, self)
        self.my_addr = None
    
    def joinComplete(self, result):
        self.my_addr = findMyIPAddr(result,
                                    config.getint(config.get('DEFAULT', 'DHT'), 'PORT'),
                                    config.getboolean('DEFAULT', 'LOCAL_OK'))
        if not self.my_addr:
            raise RuntimeError, "IP address for this machine could not be found"
        self.cache.scanDirectories()

    def joinError(self, failure):
        log.msg("joining DHT failed miserably")
        log.err(failure)
        raise RuntimeError, "IP address for this machine could not be found"
    
    def check_freshness(self, req, path, modtime, resp):
        log.msg('Checking if %s is still fresh' % path)
        d = self.peers.get('', path, method = "HEAD", modtime = modtime)
        d.addCallback(self.check_freshness_done, req, path, resp)
        return d
    
    def check_freshness_done(self, resp, req, path, orig_resp):
        if resp.code == 304:
            log.msg('Still fresh, returning: %s' % path)
            return orig_resp
        else:
            log.msg('Stale, need to redownload: %s' % path)
            return self.get_resp(req, path)
    
    def get_resp(self, req, path):
        d = defer.Deferred()
        
        log.msg('Trying to find hash for %s' % path)
        findDefer = self.mirrors.findHash(path)
        
        findDefer.addCallbacks(self.findHash_done, self.findHash_error, 
                               callbackArgs=(req, path, d), errbackArgs=(req, path, d))
        findDefer.addErrback(log.err)
        return d
    
    def findHash_error(self, failure, req, path, d):
        log.err(failure)
        self.findHash_done(HashObject(), req, path, d)
        
    def findHash_done(self, hash, req, path, d):
        if hash.expected() is None:
            log.msg('Hash for %s was not found' % path)
            self.lookupHash_done([], hash, path, d)
        else:
            log.msg('Found hash %s for %s' % (hash.hexexpected(), path))
            
            # Lookup hash in cache
            locations = self.db.lookupHash(hash.expected())
            self.getCachedFile(hash, req, path, d, locations)

    def getCachedFile(self, hash, req, path, d, locations):
        if not locations:
            log.msg('Failed to return file from cache: %s' % path)
            self.lookupHash(hash, path, d)
            return
        
        # Get the first possible location from the list
        file = locations.pop(0)['path']
        log.msg('Returning cached file: %s' % file.path)
        
        # Get it's response
        resp = static.File(file.path).renderHTTP(req)
        if isinstance(resp, defer.Deferred):
            resp.addBoth(self._getCachedFile, hash, req, path, d, locations)
        else:
            self._getCachedFile(resp, hash, req, path, d, locations)
        
    def _getCachedFile(self, resp, hash, req, path, d, locations):
        if isinstance(resp, failure.Failure):
            log.msg('Got error trying to get cached file')
            log.err()
            # Try the next possible location
            self.getCachedFile(hash, req, path, d, locations)
            return
            
        log.msg('Cached response: %r' % resp)
        
        if resp.code >= 200 and resp.code < 400:
            d.callback(resp)
        else:
            # Try the next possible location
            self.getCachedFile(hash, req, path, d, locations)

    def lookupHash(self, hash, path, d):
        log.msg('Looking up hash in DHT for file: %s' % path)
        key = hash.normexpected(bits = config.getint(config.get('DEFAULT', 'DHT'), 'HASH_LENGTH'))
        lookupDefer = self.dht.getValue(key)
        lookupDefer.addCallback(self.lookupHash_done, hash, path, d)

    def lookupHash_done(self, values, hash, path, d):
        if not values:
            log.msg('Peers for %s were not found' % path)
            getDefer = self.peers.get(hash, path)
            getDefer.addCallback(self.cache.save_file, hash, path)
            getDefer.addErrback(self.cache.save_error, path)
            getDefer.addCallbacks(d.callback, d.errback)
        else:
            log.msg('Found peers for %s: %r' % (path, values))
            # Download from the found peers
            getDefer = self.peers.get(hash, path, values)
            getDefer.addCallback(self.check_response, hash, path)
            getDefer.addCallback(self.cache.save_file, hash, path)
            getDefer.addErrback(self.cache.save_error, path)
            getDefer.addCallbacks(d.callback, d.errback)
            
    def check_response(self, response, hash, path):
        if response.code < 200 or response.code >= 300:
            log.msg('Download from peers failed, going to direct download: %s' % path)
            getDefer = self.peers.get(hash, path)
            return getDefer
        return response
        
    def new_cached_file(self, file_path, hash, url = None, forceDHT = False):
        """Add a newly cached file to the DHT.
        
        If the file was downloaded, set url to the path it was downloaded for.
        Don't add a file to the DHT unless a hash was found for it
        (but do add it anyway if forceDHT is True).
        """
        if url:
            self.mirrors.updatedFile(url, file_path)
        
        if self.my_addr and hash and (hash.expected() is not None or forceDHT):
            contact = compact(self.my_addr, config.getint('DEFAULT', 'PORT'))
            value = {'c': contact}
            key = hash.norm(bits = config.getint(config.get('DEFAULT', 'DHT'), 'HASH_LENGTH'))
            storeDefer = self.dht.storeValue(key, value)
            storeDefer.addCallback(self.store_done, hash)
            return storeDefer
        return None

    def store_done(self, result, hash):
        log.msg('Added %s to the DHT: %r' % (hash.hexdigest(), result))
        