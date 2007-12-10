
from twisted.web2.client import http
from twisted.internet.defer import Deferred

class HTTPClientManager(object):
    """A manager for all HTTP requests to a site.
    
    
    """

    implements(IHTTPClientManager)
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = http.HTTPClientProtocol(self)
        self.busy = False
        self.pipeline = False
        self.closed = False
        self.pending_requests = []
        
    def get(self, path):
        uri = 'http://' + self.host + ':' + self.port + path
        request = http.ClientRequest('GET', uri, {}, None)
        request.responseDefer = Deferred()
        self.pending_requests.append(request)
        if not self.busy:
            self._submitRequest()
        
        return request.responseDefer
    
    def _submitRequest(self):
        assert self.pending_requests
        if self.closed:
            del self.client
            self.client = http.HTTPClientProtocol(self)
        
        request = self.pending_requests.pop()
        d = self.client.submitRequest(request, False)
        d.addCallback(request.responseDefer.callback)

    def clientBusy(self, proto):
        self.busy = True

    def clientIdle(self, proto):
        self.busy = False
        if self.pending_requests:
            self._submitRequest()

    def clientPipelining(self, proto):
        self.pipeline = True

    def clientGone(self, proto):
        self.closed = True
        self.busy = False
        self.pipeline = False
        del self.client
        if self.pending_requests:
            self._submitRequest()
