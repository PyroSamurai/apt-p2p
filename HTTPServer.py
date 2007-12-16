import os.path, time

from twisted.web2 import server, http, resource, channel
from twisted.web2 import static, http_headers, responsecode

class FileDownloader(static.File):
    
    def __init__(self, path, manager, defaultType="text/plain", ignoredExts=(), processors=None, indexNames=None):
        self.manager = manager
        super(FileDownloader, self).__init__(path, defaultType, ignoredExts, processors, indexNames)
        
    def render(self, req):
        resp = super(FileDownloader, self).render(req)
        
        if self.manager:
            if resp != responsecode.NOT_FOUND:
                return self.manager.check_freshness(req.uri, resp.headers.getHeader('Last-Modified'))
            
            return self.manager.get_resp(req.uri)
        
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
                return None, ()
            
            if loc >= 0 and loc < len(self.subdirs) and self.subdirs[loc]:
                return static.File(self.subdirs[loc]), segments[1:]
            else:
                return None, ()
        
#        if len(name) > 1:
        return FileDownloader(self.directory, self.manager), segments[0:]
#        else:
#            return self, ()
        
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
