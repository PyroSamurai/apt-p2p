
import unittest

from twisted.web2.client import http
from twisted.internet.defer import Deferred
from twisted.internet.protocol import ClientFactory
from twisted.web2.client.interfaces import IHTTPClientManager
from zope.interface import implements

class HTTPClientManager(ClientFactory):
    """A manager for all HTTP requests to a site.
    
    
    """

    implements(IHTTPClientManager)
    
    protocol = HTTPClientProtocol
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = http.HTTPClientProtocol(self)
        self.busy = False
        self.pipeline = False
        self.closed = False
        self.pending_requests = []
        
    def get(self, path):
        uri = 'http://' + self.host + ':' + str(self.port) + path
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

class TestDownloader(unittest.TestCase):
    
    def test_download(self):
        h = HTTPClientManager('www.google.ca', 80)
        def print_resutls(result):
            print result
            
        d = h.get('/index.html')
        d.addCallback(print_results)
        reactor.run()
    
if __name__ == '__main__':
    unittest.main()
