
from urllib import unquote_plus

from twisted.python import log
from twisted.internet import defer
#from twisted.protocols import htb
#from twisted.protocols.policies import ThrottlingFactory
from twisted.web2 import server, http, resource, channel
from twisted.web2 import static, http_headers, responsecode

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
#            serverFilter = htb.HierarchicalBucketFilter()
#            serverBucket = htb.Bucket()
#
#            # Cap total server traffic at 20 kB/s
#            serverBucket.maxburst = 20000
#            serverBucket.rate = 20000
#
#            serverFilter.buckets[None] = serverBucket
#
#            self.factory.protocol = htb.ShapedProtocolFactory(self.factory.protocol, serverFilter)
#            self.factory = ThrottlingFactory(self.factory, writeLimit = 300*1024)
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
                log.msg('Sharing %s with %s' % (files[0]['path'].path, request.remoteAddr))
                return static.File(files[0]['path'].path), ()
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
    # Running from twistd -y
    t = TopLevel('/home', None)
    t.setDirectories({'~1': '/tmp', '~2': '/var/log'})
    factory = t.getHTTPFactory()
    
    # Standard twisted application Boilerplate
    from twisted.application import service, strports
    application = service.Application("demoserver")
    s = strports.service('tcp:18080', factory)
    s.setServiceParent(application)
