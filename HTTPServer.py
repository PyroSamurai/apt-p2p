import os.path, time
from twisted.web2 import server, http, resource, channel
from twisted.web2 import static, http_headers, responsecode

class FileDownloader(static.File):
    def render(self, req):
        resp = super(FileDownloader, self).render(req)
        if resp != responsecode.NOT_FOUND:
            return resp
        
        return http.Response(
            200,
            {'content-type': http_headers.MimeType('text', 'html')},
            """<html><body>
            <h2>Finding</h2>
            <p>TODO: eventually this will trigger a search for that file.</body></html>""")
        

class Toplevel(resource.Resource):
    addSlash = True
    
    def __init__(self, directory):
        self.directory = directory
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
        
        if len(name) > 1:
            return FileDownloader(self.directory), segments[0:]
        else:
            return self, ()
        
if __name__ == '__builtin__':
    # Running from twistd -y
    t = Toplevel('/home')
    t.addDirectory('/tmp')
    t.addDirectory('/var/log')
    site = server.Site(t)
    
    # Standard twisted application Boilerplate
    from twisted.application import service, strports
    application = service.Application("demoserver")
    s = strports.service('tcp:18080', channel.HTTPFactory(site))
    s.setServiceParent(application)
