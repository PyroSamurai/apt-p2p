
"""Serve local requests from apt and remote requests from peers."""

from urllib import quote_plus, unquote_plus
from binascii import b2a_hex
import operator

from twisted.python import log
from twisted.internet import defer
from twisted.web2 import server, http, resource, channel, stream
from twisted.web2 import static, http_headers, responsecode
from twisted.trial import unittest
from twisted.python.filepath import FilePath

from policies import ThrottlingFactory, ThrottlingProtocol, ProtocolWrapper
from apt_p2p_conf import config
from apt_p2p_Khashmir.bencode import bencode

class FileDownloader(static.File):
    """Modified to make it suitable for apt requests.
    
    Tries to find requests in the cache. Found files are first checked for
    freshness before being sent. Requests for unfound and stale files are
    forwarded to the main program for downloading.
    
    @type manager: L{apt_p2p.AptP2P}
    @ivar manager: the main program to query 
    """
    
    def __init__(self, path, manager, defaultType="text/plain", ignoredExts=(), processors=None, indexNames=None):
        self.manager = manager
        super(FileDownloader, self).__init__(path, defaultType, ignoredExts, processors, indexNames)
        
    def renderHTTP(self, req):
        log.msg('Got request for %s from %s' % (req.uri, req.remoteAddr))
        resp = super(FileDownloader, self).renderHTTP(req)
        if isinstance(resp, defer.Deferred):
            resp.addCallbacks(self._renderHTTP_done, self._renderHTTP_error,
                              callbackArgs = (req, ), errbackArgs = (req, ))
        else:
            resp = self._renderHTTP_done(resp, req)
        return resp
        
    def _renderHTTP_done(self, resp, req):
        log.msg('Initial response to %s: %r' % (req.uri, resp))
        
        if self.manager:
            path = 'http:/' + req.uri
            if resp.code >= 200 and resp.code < 400:
                return self.manager.check_freshness(req, path, resp.headers.getHeader('Last-Modified'), resp)
            
            log.msg('Not found, trying other methods for %s' % req.uri)
            return self.manager.get_resp(req, path)
        
        return resp

    def _renderHTTP_error(self, err, req):
        log.msg('Failed to render %s: %r' % (req.uri, err))
        log.err(err)
        
        if self.manager:
            path = 'http:/' + req.uri
            return self.manager.get_resp(req, path)
        
        return err

    def createSimilarFile(self, path):
        return self.__class__(path, self.manager, self.defaultType, self.ignoredExts,
                              self.processors, self.indexNames[:])
        
class FileUploaderStream(stream.FileStream):
    """Modified to make it suitable for streaming to peers.
    
    Streams the file in small chunks to make it easier to throttle the
    streaming to peers.
    
    @ivar CHUNK_SIZE: the size of chunks of data to send at a time
    """

    CHUNK_SIZE = 4*1024
    
    def read(self, sendfile=False):
        if self.f is None:
            return None

        length = self.length
        if length == 0:
            self.f = None
            return None
        
        # Remove the SendFileBuffer and mmap use, just use string reads and writes

        readSize = min(length, self.CHUNK_SIZE)

        self.f.seek(self.start)
        b = self.f.read(readSize)
        bytesRead = len(b)
        if not bytesRead:
            raise RuntimeError("Ran out of data reading file %r, expected %d more bytes" % (self.f, length))
        else:
            self.length -= bytesRead
            self.start += bytesRead
            return b


class FileUploader(static.File):
    """Modified to make it suitable for peer requests.
    
    Uses the modified L{FileUploaderStream} to stream the file for throttling,
    and doesn't do any listing of directory contents.
    """

    def render(self, req):
        if not self.fp.exists():
            return responsecode.NOT_FOUND

        if self.fp.isdir():
            # Don't try to render a directory listing
            return responsecode.NOT_FOUND

        try:
            f = self.fp.open()
        except IOError, e:
            import errno
            if e[0] == errno.EACCES:
                return responsecode.FORBIDDEN
            elif e[0] == errno.ENOENT:
                return responsecode.NOT_FOUND
            else:
                raise

        response = http.Response()
        # Use the modified FileStream
        response.stream = FileUploaderStream(f, 0, self.fp.getsize())

        for (header, value) in (
            ("content-type", self.contentType()),
            ("content-encoding", self.contentEncoding()),
        ):
            if value is not None:
                response.headers.setHeader(header, value)

        return response

class UploadThrottlingProtocol(ThrottlingProtocol):
    """Protocol for throttling uploads.
    
    Determines whether or not to throttle the upload based on the type of stream.
    Uploads use L{FileUploaderStream} or L{twisted.web2.stream.MemorySTream},
    apt uses L{CacheManager.ProxyFileStream} or L{twisted.web.stream.FileStream}.
    """
    
    stats = None

    def __init__(self, factory, wrappedProtocol):
        ThrottlingProtocol.__init__(self, factory, wrappedProtocol)
        self.throttle = False

    def write(self, data):
        if self.throttle:
            ThrottlingProtocol.write(self, data)
            if stats:
                stats.sentBytes(len(data))
        else:
            ProtocolWrapper.write(self, data)

    def writeSequence(self, seq):
        if self.throttle:
            ThrottlingProtocol.writeSequence(self, seq)
            if stats:
                stats.sentBytes(reduce(operator.add, map(len, seq)))
        else:
            ProtocolWrapper.writeSequence(self, seq)

    def registerProducer(self, producer, streaming):
        ThrottlingProtocol.registerProducer(self, producer, streaming)
        streamType = getattr(producer, 'stream', None)
        if isinstance(streamType, FileUploaderStream) or isinstance(streamType, stream.MemoryStream):
            self.throttle = True


class TopLevel(resource.Resource):
    """The HTTP server for all requests, both from peers and apt.
    
    @type directory: L{twisted.python.filepath.FilePath}
    @ivar directory: the directory to check for cached files
    @type db: L{db.DB}
    @ivar db: the database to use for looking up files and hashes
    @type manager: L{apt_p2p.AptP2P}
    @ivar manager: the main program object to send requests to
    @type factory: L{twisted.web2.channel.HTTPFactory} or L{policies.ThrottlingFactory}
    @ivar factory: the factory to use to serve HTTP requests
    """
    
    addSlash = True
    
    def __init__(self, directory, db, manager):
        """Initialize the instance.
        
        @type directory: L{twisted.python.filepath.FilePath}
        @param directory: the directory to check for cached files
        @type db: L{db.DB}
        @param db: the database to use for looking up files and hashes
        @type manager: L{apt_p2p.AptP2P}
        @param manager: the main program object to send requests to
        """
        self.directory = directory
        self.db = db
        self.manager = manager
        self.uploadLimit = None
        if config.getint('DEFAULT', 'UPLOAD_LIMIT') > 0:
            self.uploadLimit = int(config.getint('DEFAULT', 'UPLOAD_LIMIT')*1024)
        self.factory = None

    def getHTTPFactory(self):
        """Initialize and get the factory for this HTTP server."""
        if self.factory is None:
            self.factory = channel.HTTPFactory(server.Site(self),
                                               **{'maxPipeline': 10, 
                                                  'betweenRequestsTimeOut': 60})
            self.factory = ThrottlingFactory(self.factory, writeLimit = self.uploadLimit)
            self.factory.protocol = UploadThrottlingProtocol
            self.factory.protocol.stats = self.manager.stats
        return self.factory

    def render(self, ctx):
        """Render a web page with descriptive statistics."""
        if self.manager:
            return http.Response(
                200,
                {'content-type': http_headers.MimeType('text', 'html')},
                self.manager.getStats())
        else:
            return http.Response(
                200,
                {'content-type': http_headers.MimeType('text', 'html')},
                '<html><body><p>Some Statistics</body></html>')

    def locateChild(self, request, segments):
        """Process the incoming request."""
        log.msg('Got HTTP request for %s from %s' % (request.uri, request.remoteAddr))
        name = segments[0]
        
        # If the request is for a shared file (from a peer)
        if name == '~':
            if len(segments) != 2:
                log.msg('Got a malformed request from %s' % request.remoteAddr)
                return None, ()
            
            # Find the file in the database
            # Have to unquote_plus the uri, because the segments are unquoted by twisted
            hash = unquote_plus(request.uri[3:])
            files = self.db.lookupHash(hash)
            if files:
                # If it is a file, return it
                if 'path' in files[0]:
                    log.msg('Sharing %s with %s' % (files[0]['path'].path, request.remoteAddr))
                    return FileUploader(files[0]['path'].path), ()
                else:
                    # It's not for a file, but for a piece string, so return that
                    log.msg('Sending torrent string %s to %s' % (b2a_hex(hash), request.remoteAddr))
                    return static.Data(bencode({'t': files[0]['pieces']}), 'application/x-bencoded'), ()
            else:
                log.msg('Hash could not be found in database: %r' % hash)

        # Only local requests (apt) get past this point
        if request.remoteAddr.host != "127.0.0.1":
            log.msg('Blocked illegal access to %s from %s' % (request.uri, request.remoteAddr))
            return None, ()
        
        # Block access to index .diff files (for now)
        if 'Packages.diff' in segments or 'Sources.diff' in segments:
            return None, ()
         
        if len(name) > 1:
            # It's a request from apt
            return FileDownloader(self.directory.path, self.manager), segments[0:]
        else:
            # Will render the statistics page
            return self, ()
        
        log.msg('Got a malformed request for "%s" from %s' % (request.uri, request.remoteAddr))
        return None, ()

class TestTopLevel(unittest.TestCase):
    """Unit tests for the HTTP Server."""
    
    client = None
    pending_calls = []
    torrent_hash = '\xca \xb8\x0c\x00\xe7\x07\xf8~])+\x9d\xe5_B\xff\x1a\xc4!'
    torrent = 'abcdefghij0123456789\xca\xec\xb8\x0c\x00\xe7\x07\xf8~])\x8f\x9d\xe5_B\xff\x1a\xc4!'
    file_hash = '\xf8~])+\x9d\xe5_B\xff\x1a\xc4!\xca \xb8\x0c\x00\xe7\x07'
    
    def setUp(self):
        self.client = TopLevel(FilePath('/boot'), self, None)
        
    def lookupHash(self, hash):
        if hash == self.torrent_hash:
            return [{'pieces': self.torrent}]
        elif hash == self.file_hash:
            return [{'path': FilePath('/boot/grub/stage2')}]
        else:
            return []
        
    def create_request(self, host, path):
        req = server.Request(None, 'GET', path, (1,1), 0, http_headers.Headers())
        class addr:
            host = ''
            port = 0
        req.remoteAddr = addr()
        req.remoteAddr.host = host
        req.remoteAddr.port = 23456
        server.Request._parseURL(req)
        return req
        
    def test_unauthorized(self):
        req = self.create_request('128.0.0.1', '/foo/bar')
        self.failUnlessRaises(http.HTTPError, req._getChild, None, self.client, req.postpath)
        
    def test_Packages_diff(self):
        req = self.create_request('127.0.0.1',
                '/ftp.us.debian.org/debian/dists/unstable/main/binary-i386/Packages.diff/Index')
        self.failUnlessRaises(http.HTTPError, req._getChild, None, self.client, req.postpath)
        
    def test_Statistics(self):
        req = self.create_request('127.0.0.1', '/')
        res = req._getChild(None, self.client, req.postpath)
        self.failIfEqual(res, None)
        df = defer.maybeDeferred(res.renderHTTP, req)
        df.addCallback(self.check_resp, 200)
        return df
        
    def test_apt_download(self):
        req = self.create_request('127.0.0.1',
                '/ftp.us.debian.org/debian/dists/stable/Release')
        res = req._getChild(None, self.client, req.postpath)
        self.failIfEqual(res, None)
        self.failUnless(isinstance(res, FileDownloader))
        df = defer.maybeDeferred(res.renderHTTP, req)
        df.addCallback(self.check_resp, 404)
        return df
        
    def test_torrent_upload(self):
        req = self.create_request('123.45.67.89',
                                  '/~/' + quote_plus(self.torrent_hash))
        res = req._getChild(None, self.client, req.postpath)
        self.failIfEqual(res, None)
        self.failUnless(isinstance(res, static.Data))
        df = defer.maybeDeferred(res.renderHTTP, req)
        df.addCallback(self.check_resp, 200)
        return df
        
    def test_file_upload(self):
        req = self.create_request('123.45.67.89',
                                  '/~/' + quote_plus(self.file_hash))
        res = req._getChild(None, self.client, req.postpath)
        self.failIfEqual(res, None)
        self.failUnless(isinstance(res, FileUploader))
        df = defer.maybeDeferred(res.renderHTTP, req)
        df.addCallback(self.check_resp, 200)
        return df
    
    def test_missing_hash(self):
        req = self.create_request('123.45.67.89',
                                  '/~/' + quote_plus('foobar'))
        self.failUnlessRaises(http.HTTPError, req._getChild, None, self.client, req.postpath)

    def check_resp(self, resp, code):
        self.failUnlessEqual(resp.code, code)
        return resp
        
    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.pending_calls = []
        if self.client:
            self.client = None

if __name__ == '__builtin__':
    # Running from twistd -ny HTTPServer.py
    # Then test with:
    #   wget -S 'http://localhost:18080/~/whatever'
    #   wget -S 'http://localhost:18080/~/pieces'

    import os.path
    from twisted.python.filepath import FilePath
    
    class DB:
        def lookupHash(self, hash):
            if hash == 'pieces':
                return [{'pieces': 'abcdefghij0123456789\xca\xec\xb8\x0c\x00\xe7\x07\xf8~])\x8f\x9d\xe5_B\xff\x1a\xc4!'}]
            return [{'path': FilePath(os.path.expanduser('~/school/optout'))}]
    
    t = TopLevel(FilePath(os.path.expanduser('~')), DB(), None, 0)
    factory = t.getHTTPFactory()
    
    # Standard twisted application Boilerplate
    from twisted.application import service, strports
    application = service.Application("demoserver")
    s = strports.service('tcp:18080', factory)
    s.setServiceParent(application)
