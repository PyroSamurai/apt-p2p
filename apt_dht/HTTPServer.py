
from twisted.python import log
from twisted.internet import defer
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
    
    def __init__(self, directory, manager):
        self.directory = directory
        self.manager = manager
        self.subdirs = {}
        self.factory = None

    def getHTTPFactory(self):
        if self.factory is None:
            self.factory = channel.HTTPFactory(server.Site(self),
                                               **{'maxPipeline': 10, 
                                                  'betweenRequestsTimeOut': 60})
        return self.factory

    def setDirectories(self, dirs):
        self.subdirs = {}
        for k in dirs:
            # Don't allow empty subdirectory
            if k:
                self.subdirs[k] = dirs[k]
        log.msg('new subdirectories initialized')
    
    def render(self, ctx):
        return http.Response(
            200,
            {'content-type': http_headers.MimeType('text', 'html')},
            """<html><body>
            <h2>Statistics</h2>
            <p>TODO: eventually some stats will be shown here.</body></html>""")

    def locateChild(self, request, segments):
        name = segments[0]
        if name in self.subdirs:
            log.msg('Sharing %s with %s' % (request.uri, request.remoteAddr))
            return static.File(self.subdirs[name].path), segments[1:]
        
        if request.remoteAddr.host != "127.0.0.1":
            log.msg('Blocked illegal access to %s from %s' % (request.uri, request.remoteAddr))
            return None, ()
            
        if len(name) > 1:
            return FileDownloader(self.directory.path, self.manager), segments[0:]
        else:
            return self, ()

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
