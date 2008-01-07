import os.path, time

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
                return self.manager.check_freshness(path, resp.headers.getHeader('Last-Modified'), resp)
            
            log.msg('Not found, trying other methods for %s' % req.uri)
            return self.manager.get_resp(path)
        
        return resp

    def createSimilarFile(self, path):
        return self.__class__(path, self.manager, self.defaultType, self.ignoredExts,
                              self.processors, self.indexNames[:])
        
        
class TopLevel(resource.Resource):
    addSlash = True
    
    def __init__(self, directory, manager):
        self.directory = directory
        self.manager = manager
        self.subdirs = []

    def addDirectory(self, directory):
        path = "~" + str(len(self.subdirs))
        self.subdirs.append(directory)
        return path
    
    def removeDirectory(self, directory):
        loc = self.subdirs.index(directory)
        self.subdirs[loc] = ''
        
    def render(self, ctx):
        return http.Response(
            200,
            {'content-type': http_headers.MimeType('text', 'html')},
            """<html><body>
            <h2>Statistics</h2>
            <p>TODO: eventually some stats will be shown here.</body></html>""")

    def locateChild(self, request, segments):
        name = segments[0]
        if len(name) > 1 and name[0] == '~':
            try:
                loc = int(name[1:])
            except:
                log.msg('Not found: %s from %s' % (request.uri, request.remoteAddr))
                return None, ()
            
            if loc >= 0 and loc < len(self.subdirs) and self.subdirs[loc]:
                log.msg('Sharing %s with %s' % (request.uri, request.remoteAddr))
                return static.File(self.subdirs[loc]), segments[1:]
            else:
                log.msg('Not found: %s from %s' % (request.uri, request.remoteAddr))
                return None, ()
        
        if request.remoteAddr.host != "127.0.0.1":
            log.msg('Blocked illegal access to %s from %s' % (request.uri, request.remoteAddr))
            return None, ()
            
        if len(name) > 1:
            return FileDownloader(self.directory, self.manager), segments[0:]
        else:
            return self, ()
        
if __name__ == '__builtin__':
    # Running from twistd -y
    t = TopLevel('/home', None)
    t.addDirectory('/tmp')
    t.addDirectory('/var/log')
    site = server.Site(t)
    
    # Standard twisted application Boilerplate
    from twisted.application import service, strports
    application = service.Application("demoserver")
    s = strports.service('tcp:18080', channel.HTTPFactory(site))
    s.setServiceParent(application)
