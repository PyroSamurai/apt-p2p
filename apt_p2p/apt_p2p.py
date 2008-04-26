
"""The main program code.

@var download_dir: the name of the directory to use for downloaded files
@var peer_dir: the name of the directory to use for peer downloads
"""

from urllib import unquote

from twisted.internet import defer, reactor, protocol
from twisted.web2 import static
from twisted.python import log, failure
from twisted.python.filepath import FilePath

from apt_p2p_conf import config
from DHTManager import DHT
from PeerManager import PeerManager
from HTTPServer import TopLevel
from MirrorManager import MirrorManager
from CacheManager import CacheManager
from Hash import HashObject
from db import DB
from stats import StatsLogger

download_dir = 'cache'
peer_dir = 'peers'

class AptP2P(protocol.Factory):
    """The main code object that does all of the work.
    
    Contains all of the sub-components that do all the low-level work, and
    coordinates communication between them.
    
    @type dhtClass: L{interfaces.IDHT}
    @ivar dhtClass: the DHT class to use
    @type cache_dir: L{twisted.python.filepath.FilePath}
    @ivar cache_dir: the directory to use for storing all files
    @type db: L{db.DB}
    @ivar db: the database to use for tracking files and hashes
    @type dht: L{DHTManager.DHT}
    @ivar dht: the manager for DHT requests
    @type stats: L{stats.StatsLogger}
    @ivar stats: the statistics logger to record sent data to
    @type http_server: L{HTTPServer.TopLevel}
    @ivar http_server: the web server that will handle all requests from apt
        and from other peers
    @type peers: L{PeerManager.PeerManager}
    @ivar peers: the manager of all downloads from mirrors and other peers
    @type mirrors: L{MirrorManager.MirrorManager}
    @ivar mirrors: the manager of downloaded information about mirrors which
        can be queried to get hashes from file names
    @type cache: L{CacheManager.CacheManager}
    @ivar cache: the manager of all downloaded files
    @type my_addr: C{string}, C{int}
    @ivar my_addr: the IP address and port of this peer
    """
    
    def __init__(self, dhtClass):
        """Initialize all the sub-components.
        
        @type dhtClass: L{interfaces.IDHT}
        @param dhtClass: the DHT class to use
        """
        log.msg('Initializing the main apt_p2p application')
        self.dhtClass = dhtClass
        self.my_addr = None

    #{ Factory interface
    def startFactory(self):
        reactor.callLater(0, self._startFactory)
        
    def _startFactory(self):
        log.msg('Starting the main apt_p2p application')
        self.cache_dir = FilePath(config.get('DEFAULT', 'CACHE_DIR'))
        if not self.cache_dir.child(download_dir).exists():
            self.cache_dir.child(download_dir).makedirs()
        if not self.cache_dir.child(peer_dir).exists():
            self.cache_dir.child(peer_dir).makedirs()
        self.db = DB(self.cache_dir.child('apt-p2p.db'))
        self.dht = DHT(self.dhtClass, self.db)
        df = self.dht.start()
        df.addCallback(self._dhtStarted)
        self.stats = StatsLogger(self.db)
        self.http_server = TopLevel(self.cache_dir.child(download_dir), self.db, self)
        self.http_server.getHTTPFactory().startFactory()
        self.peers = PeerManager(self.cache_dir.child(peer_dir), self.dht, self.stats)
        self.mirrors = MirrorManager(self.cache_dir)
        self.cache = CacheManager(self.cache_dir.child(download_dir), self.db, self)
    
    def _dhtStarted(self, result):
        """Save the returned address and start scanning the cache."""
        self.my_addr = result
        self.cache.scanDirectories()
        
    def stopFactory(self):
        log.msg('Stoppping the main apt_p2p application')
        self.http_server.getHTTPFactory().stopFactory()
        self.mirrors.cleanup()
        self.stats.save()
        self.db.close()
    
    def buildProtocol(self, addr):
        return self.http_server.getHTTPFactory().buildProtocol(addr)

    #{ Other functions
    def getStats(self):
        """Retrieve and format the statistics for the program.
        
        @rtype: C{string}
        @return: the formatted HTML page containing the statistics
        """
        out = '<html><body>\n\n'
        out += self.stats.formatHTML(self.my_addr)
        out += '\n\n'
        out += self.dht.getStats()
        out += '\n</body></html>\n'
        return out

    #{ Main workflow
    def get_resp(self, req, url, orig_resp = None):
        """Lookup a hash for the file in the local mirror info.
        
        Starts the process of getting a response to an apt request.
        
        @type req: L{twisted.web2.http.Request}
        @param req: the initial request sent to the HTTP server by apt
        @param url: the URI of the actual mirror request
        @type orig_resp: L{twisted.web2.http.Response}
        @param orig_resp: the response from the cache to be sent to apt
            (optional, ignored if missing)
        @rtype: L{twisted.internet.defer.Deferred}
        @return: a deferred that will be called back with the response
        """
        d = defer.Deferred()
        
        log.msg('Trying to find hash for %s' % url)
        findDefer = self.mirrors.findHash(unquote(url))
        
        findDefer.addCallbacks(self.findHash_done, self.findHash_error, 
                               callbackArgs=(req, url, orig_resp, d),
                               errbackArgs=(req, url, orig_resp, d))
        return d
    
    def findHash_error(self, failure, req, url, orig_resp, d):
        """Process the error in hash lookup by returning an empty L{HashObject}."""
        log.msg('Hash lookup for %s resulted in an error: %s' %
                (url, failure.getErrorMessage()))
        self.findHash_done(HashObject(), req, url, orig_resp, d)
        
    def findHash_done(self, hash, req, url, orig_resp, d):
        """Use the returned hash to lookup the file in the cache.
        
        If the hash was not found, the workflow skips down to download from
        the mirror (L{startDownload}), or checks the freshness of an old
        response if there is one.
        
        @type hash: L{Hash.HashObject}
        @param hash: the hash object containing the expected hash for the file
        """
        if hash.expected() is None:
            log.msg('Hash for %s was not found' % url)
            # Send the old response or get a new one
            if orig_resp:
                self.check_freshness(req, url, orig_resp, d)
            else:
                self.startDownload([], req, hash, url, d)
        else:
            log.msg('Found hash %s for %s' % (hash.hexexpected(), url))
            
            # Lookup hash in cache
            locations = self.db.lookupHash(hash.expected(), filesOnly = True)
            self.getCachedFile(hash, req, url, d, locations)

    def check_freshness(self, req, url, orig_resp, d):
        """Send a HEAD to the mirror to check if the response from the cache is still valid.
        
        @type req: L{twisted.web2.http.Request}
        @param req: the initial request sent to the HTTP server by apt
        @param url: the URI of the actual mirror request
        @type orig_resp: L{twisted.web2.http.Response}
        @param orig_resp: the response from the cache to be sent to apt
        """
        log.msg('Checking if %s is still fresh' % url)
        modtime = orig_resp.headers.getHeader('Last-Modified')
        headDefer = self.peers.get(HashObject(), url, method = "HEAD",
                                   modtime = modtime)
        headDefer.addCallbacks(self.check_freshness_done,
                               self.check_freshness_error,
                               callbackArgs = (req, url, orig_resp, d),
                               errbackArgs = (req, url, d))
    
    def check_freshness_done(self, resp, req, url, orig_resp, d):
        """Return the fresh response, if stale start to redownload.
        
        @type resp: L{twisted.web2.http.Response}
        @param resp: the response from the mirror to the HEAD request
        @type req: L{twisted.web2.http.Request}
        @param req: the initial request sent to the HTTP server by apt
        @param url: the URI of the actual mirror request
        @type orig_resp: L{twisted.web2.http.Response}
        @param orig_resp: the response from the cache to be sent to apt
        """
        if resp.code == 304:
            log.msg('Still fresh, returning: %s' % url)
            d.callback(orig_resp)
        else:
            log.msg('Stale, need to redownload: %s' % url)
            self.startDownload([], req, HashObject(), url, d)
    
    def check_freshness_error(self, err, req, url, d):
        """Mirror request failed, continue with download.
        
        @param err: the response from the mirror to the HEAD request
        @type req: L{twisted.web2.http.Request}
        @param req: the initial request sent to the HTTP server by apt
        @param url: the URI of the actual mirror request
        """
        log.err(err)
        self.startDownload([], req, HashObject(), url, d)
    
    def getCachedFile(self, hash, req, url, d, locations):
        """Try to return the file from the cache, otherwise move on to a DHT lookup.
        
        @type locations: C{list} of C{dictionary}
        @param locations: the files in the cache that match the hash,
            the dictionary contains a key 'path' whose value is a
            L{twisted.python.filepath.FilePath} object for the file.
        """
        if not locations:
            log.msg('Failed to return file from cache: %s' % url)
            self.lookupHash(req, hash, url, d)
            return
        
        # Get the first possible location from the list
        file = locations.pop(0)['path']
        log.msg('Returning cached file: %s' % file.path)
        
        # Get it's response
        resp = static.File(file.path).renderHTTP(req)
        if isinstance(resp, defer.Deferred):
            resp.addBoth(self._getCachedFile, hash, req, url, d, locations)
        else:
            self._getCachedFile(resp, hash, req, url, d, locations)
        
    def _getCachedFile(self, resp, hash, req, url, d, locations):
        """Check the returned response to be sure it is valid."""
        if isinstance(resp, failure.Failure):
            log.msg('Got error trying to get cached file')
            log.err(resp)
            # Try the next possible location
            self.getCachedFile(hash, req, url, d, locations)
            return
            
        log.msg('Cached response: %r' % resp)
        
        if resp.code >= 200 and resp.code < 400:
            d.callback(resp)
        else:
            # Try the next possible location
            self.getCachedFile(hash, req, url, d, locations)

    def lookupHash(self, req, hash, url, d):
        """Lookup the hash in the DHT."""
        log.msg('Looking up hash in DHT for file: %s' % url)
        key = hash.expected()
        lookupDefer = self.dht.get(key)
        lookupDefer.addBoth(self.startDownload, req, hash, url, d)

    def startDownload(self, values, req, hash, url, d):
        """Start the download of the file.
        
        The download will be from peers if the DHT lookup succeeded, or
        from the mirror otherwise.
        
        @type values: C{list} of C{dictionary}
        @param values: the returned values from the DHT containing peer
            download information
        """
        # Remove some headers Apt sets in the request
        req.headers.removeHeader('If-Modified-Since')
        req.headers.removeHeader('Range')
        req.headers.removeHeader('If-Range')
        
        if not isinstance(values, list) or not values:
            if not isinstance(values, list):
                log.msg('DHT lookup for %s failed with error %r' % (url, values))
            else:
                log.msg('Peers for %s were not found' % url)
            getDefer = self.peers.get(hash, url)
#            getDefer.addErrback(self.final_fallback, hash, url)
            getDefer.addCallback(self.cache.save_file, hash, url)
            getDefer.addErrback(self.cache.save_error, url)
            getDefer.addCallbacks(d.callback, d.errback)
        else:
            log.msg('Found peers for %s: %r' % (url, values))
            # Download from the found peers
            getDefer = self.peers.get(hash, url, values)
            getDefer.addCallback(self.check_response, hash, url)
            getDefer.addCallback(self.cache.save_file, hash, url)
            getDefer.addErrback(self.cache.save_error, url)
            getDefer.addCallbacks(d.callback, d.errback)
            
    def check_response(self, response, hash, url):
        """Check the response from peers, and download from the mirror if it is not."""
        if response.code < 200 or response.code >= 300:
            log.msg('Download from peers failed, going to direct download: %s' % url)
            getDefer = self.peers.get(hash, url)
#            getDefer.addErrback(self.final_fallback, hash, url)
            return getDefer
        return response
        
    def final_fallback(self, err, hash, url):
        """Final retry if the mirror still generated an error."""
        log.msg('Download from mirror failed, retrying once only: %s' % url)
        log.err(err)
        getDefer = self.peers.get(hash, url)
        return getDefer
        
    def new_cached_file(self, file_path, hash, new_hash, url = None, forceDHT = False):
        """Add a newly cached file to the mirror info and/or the DHT.
        
        If the file was downloaded, set url to the path it was downloaded for.
        Doesn't add a file to the DHT unless a hash was found for it
        (but does add it anyway if forceDHT is True).
        
        @type file_path: L{twisted.python.filepath.FilePath}
        @param file_path: the location of the file in the local cache
        @type hash: L{Hash.HashObject}
        @param hash: the original (expected) hash object containing also the
            hash of the downloaded file
        @type new_hash: C{boolean}
        @param new_hash: whether the has was new to this peer, and so should
            be added to the DHT
        @type url: C{string}
        @param url: the URI of the location of the file in the mirror
            (optional, defaults to not adding the file to the mirror info)
        @type forceDHT: C{boolean}
        @param forceDHT: whether to force addition of the file to the DHT
            even if the hash was not found in a mirror
            (optional, defaults to False)
        """
        if url:
            self.mirrors.updatedFile(url, file_path)
        
        if self.my_addr and hash and new_hash and (hash.expected() is not None or forceDHT):
            return self.dht.store(hash)
        return None
    