
from urllib import unquote_plus
from binascii import b2a_hex

from twisted.python import log
from twisted.internet import defer
from twisted.web2 import server, http, resource, channel, stream
from twisted.web2 import static, http_headers, responsecode

from policies import ThrottlingFactory
from apt_dht_Khashmir.bencode import bencode

class FileDownloader(static.File):
    
    def __init__(self, path, manager, defaultType="text/plain", ignoredExts=(), processors=None, indexNames=None):
        self.manager = manager
        super(FileDownloader, self).__init__(path, defaultType, ignoredExts, processors, indexNames)
        
    def renderHTTP(self, req):
        log.msg('Got request for %s from %s' % (req.uri, req.remoteAddr))
        resp = super(FileDownloader, self).renderHTTP(req)
        if isinstance(resp, defer.Deferred):
            resp.addCallback(self._renderHTTP_done, req)
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

    def createSimilarFile(self, path):
        return self.__class__(path, self.manager, self.defaultType, self.ignoredExts,
                              self.processors, self.indexNames[:])
        
class FileUploaderStream(stream.FileStream):

    CHUNK_SIZE = 4*1024
    
    def read(self, sendfile=False):
        if self.f is None:
            return None

        length = self.length
        if length == 0:
            self.f = None
            return None

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

    def render(self, req):
        if not self.fp.exists():
            return responsecode.NOT_FOUND

        if self.fp.isdir():
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
        response.stream = FileUploaderStream(f, 0, self.fp.getsize())

        for (header, value) in (
            ("content-type", self.contentType()),
            ("content-encoding", self.contentEncoding()),
        ):
            if value is not None:
                response.headers.setHeader(header, value)

        return response

class TopLevel(resource.Resource):
    addSlash = True
    
    def __init__(self, directory, db, manager):
        self.directory = directory
        self.db = db
        self.manager = manager
        self.factory = None

    def getHTTPFactory(self):
        if self.factory is None:
            self.factory = channel.HTTPFactory(server.Site(self),
                                               **{'maxPipeline': 10, 
                                                  'betweenRequestsTimeOut': 60})
            self.factory = ThrottlingFactory(self.factory, writeLimit = 30*1024)
        return self.factory

    def render(self, ctx):
        return http.Response(
            200,
            {'content-type': http_headers.MimeType('text', 'html')},
            """<html><body>
            <h2>Statistics</h2>
            <p>TODO: eventually some stats will be shown here.</body></html>""")

    def locateChild(self, request, segments):
        log.msg('Got HTTP request for %s from %s' % (request.uri, request.remoteAddr))
        name = segments[0]
        if name == '~':
            if len(segments) != 2:
                log.msg('Got a malformed request from %s' % request.remoteAddr)
                return None, ()
            hash = unquote_plus(segments[1])
            files = self.db.lookupHash(hash)
            if files:
                if 'path' in files[0]:
                    log.msg('Sharing %s with %s' % (files[0]['path'].path, request.remoteAddr))
                    return FileUploader(files[0]['path'].path), ()
                else:
                    log.msg('Sending torrent string %s to %s' % (b2a_hex(hash), request.remoteAddr))
                    return static.Data(bencode({'t': files[0]['pieces']}), 'application/bencoded'), ()
            else:
                log.msg('Hash could not be found in database: %s' % hash)
        
        if request.remoteAddr.host != "127.0.0.1":
            log.msg('Blocked illegal access to %s from %s' % (request.uri, request.remoteAddr))
            return None, ()
            
        if len(name) > 1:
            return FileDownloader(self.directory.path, self.manager), segments[0:]
        else:
            return self, ()
        
        log.msg('Got a malformed request for "%s" from %s' % (request.uri, request.remoteAddr))
        return None, ()

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
    
    t = TopLevel(FilePath(os.path.expanduser('~')), DB(), None)
    factory = t.getHTTPFactory()
    
    # Standard twisted application Boilerplate
    from twisted.application import service, strports
    application = service.Application("demoserver")
    s = strports.service('tcp:18080', factory)
    s.setServiceParent(application)
