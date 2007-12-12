
from twisted.internet import reactor, defer, protocol
from twisted.internet.protocol import ClientFactory
from twisted.web2.client.interfaces import IHTTPClientManager
from twisted.web2.client.http import ProtocolError, ClientRequest, HTTPClientProtocol
from twisted.trial import unittest
from zope.interface import implements

class HTTPClientManager(ClientFactory):
    """A manager for all HTTP requests to a single site.
    
    Controls all requests that got to a single site (host and port).
    This includes buffering requests until they can be sent and reconnecting
    in the even of the connection being closed.
    
    """

    implements(IHTTPClientManager)
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.busy = False
        self.pipeline = False
        self.closed = True
        self.connecting = False
        self.request_queue = []
        self.response_queue = []
        self.proto = None
        self.connector = None
        
    def connect(self):
        assert(self.closed and not self.connecting)
        self.connecting = True
        d = protocol.ClientCreator(reactor, HTTPClientProtocol, self).connectTCP(self.host, self.port)
        d.addCallback(self.connected)

    def connected(self, proto):
        self.closed = False
        self.connecting = False
        self.proto = proto
        self.processQueue()
        
    def close(self):
        if not self.closed:
            self.proto.transport.loseConnection()

    def is_idle(self):
        return not self.busy and not self.request_queue and not self.response_queue
    
    def submitRequest(self, request):
        request.deferRequest = defer.Deferred()
        self.request_queue.append(request)
        self.processQueue()
        return request.deferRequest

    def processQueue(self):
        if not self.request_queue:
            return
        if self.connecting:
            return
        if self.closed:
            self.connect()
            return
        if self.busy and not self.pipeline:
            return
        if self.response_queue and not self.pipeline:
            return

        req = self.request_queue.pop(0)
        self.response_queue.append(req)
        req.deferResponse = self.proto.submitRequest(req, False)
        req.deferResponse.addCallback(self.requestComplete)
        req.deferResponse.addErrback(self.requestError)

    def requestComplete(self, resp):
        req = self.response_queue.pop(0)
        req.deferRequest.callback(resp)

    def requestError(self, error):
        req = self.response_queue.pop(0)
        req.deferRequest.errback(error)

    def clientBusy(self, proto):
        self.busy = True

    def clientIdle(self, proto):
        self.busy = False
        self.processQueue()

    def clientPipelining(self, proto):
        self.pipeline = True
        self.processQueue()

    def clientGone(self, proto):
        for req in self.response_queue:
            req.deferRequest.errback(ProtocolError('lost connection'))
        self.busy = False
        self.pipeline = False
        self.closed = True
        self.connecting = False
        self.response_queue = []
        self.proto = None
        if self.request_queue:
            self.processQueue()
            
class HTTPDownloader:
    """Manages all the HTTP connections to various sites."""
    
    def __init__(self):
        self.clients = {}
    
    def get(self, host, port, request):
        site = host + ":" + str(port)
        if site not in self.clients:
            self.clients[site] = HTTPClientManager(host, port)
        return self.clients[site].submitRequest(request)
    
    def closeAll(self):
        for site in self.clients:
            self.clients[site].close()
        self.clients = {}

class TestClientManager(unittest.TestCase):
    """Unit tests for the HTTPClientManager."""
    
    client = None
    pending_calls = []
    
    def gotResp(self, resp, num, expect):
        self.failUnless(resp.code >= 200 and resp.code < 300, "Got a non-200 response: %r" % resp.code)
        self.failUnless(resp.stream.length == expect, "Length was incorrect, got %r, expected %r" % (resp.stream.length, expect))
        resp.stream.close()
    
    def test_download(self):
        host = 'www.camrdale.org'
        self.client = HTTPClientManager(host, 80)
        self.timeout = 10
        lastDefer = defer.Deferred()
        
        d = self.client.submitRequest(ClientRequest("GET", '/robots.txt', {'Host':host}, None))
        d.addCallback(self.gotResp, 1, 309)
        d.addBoth(lastDefer.callback)
        return lastDefer
        
    def test_head(self):
        host = 'www.camrdale.org'
        self.client = HTTPClientManager(host, 80)
        self.timeout = 10
        lastDefer = defer.Deferred()
        
        d = self.client.submitRequest(ClientRequest("HEAD", '/robots.txt', {'Host':host}, None))
        d.addCallback(self.gotResp, 1, 0)
        d.addBoth(lastDefer.callback)
        return lastDefer
        
    def test_multiple_downloads(self):
        host = 'www.camrdale.org'
        self.client = HTTPClientManager(host, 80)
        self.timeout = 120
        lastDefer = defer.Deferred()
        
        def newRequest(path, num, expect, last=False):
            d = self.client.submitRequest(ClientRequest("GET", path, {'Host':host}, None))
            d.addCallback(self.gotResp, num, expect)
            if last:
                d.addCallback(lastDefer.callback)
                
        newRequest("/", 1, 3433)
        newRequest("/blog/", 2, 37121)
        newRequest("/camrdale.html", 3, 2234)
        self.pending_calls.append(reactor.callLater(1, newRequest, '/robots.txt', 4, 309))
        self.pending_calls.append(reactor.callLater(10, newRequest, '/wikilink.html', 5, 3084))
        self.pending_calls.append(reactor.callLater(30, newRequest, '/sitemap.html', 6, 4750))
        self.pending_calls.append(reactor.callLater(31, newRequest, '/PlanetLab.html', 7, 2783))
        self.pending_calls.append(reactor.callLater(32, newRequest, '/openid.html', 8, 2525))
        self.pending_calls.append(reactor.callLater(32, newRequest, '/subpage.html', 9, 2381))
        self.pending_calls.append(reactor.callLater(62, newRequest, '/sitemap2.rss', 0, 302362, True))
        return lastDefer
        
    def test_range(self):
        host = 'www.camrdale.org'
        self.client = HTTPClientManager(host, 80)
        self.timeout = 10
        lastDefer = defer.Deferred()
        
        d = self.client.submitRequest(ClientRequest("GET", '/robots.txt', {'Host':host, 'Range': ('bytes', [(100, 199)])}, None))
        d.addCallback(self.gotResp, 1, 100)
        d.addBoth(lastDefer.callback)
        return lastDefer
        
    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.pending_calls = []
        if self.client:
            self.client.close()
            self.client = None

class TestDownloader(unittest.TestCase):
    """Unit tests for the HTTPDownloader."""
    
    manager = None
    pending_calls = []
    
    def gotResp(self, resp, num, expect):
        self.failUnless(resp.code >= 200 and resp.code < 300, "Got a non-200 response: %r" % resp.code)
        if expect is not None:
            self.failUnless(resp.stream.length == expect, "Length was incorrect, got %r, expected %r" % (resp.stream.length, expect))
        resp.stream.close()
    
    def test_download(self):
        self.manager = HTTPDownloader()
        self.timeout = 10
        lastDefer = defer.Deferred()
        
        host = 'www.camrdale.org'
        d = self.manager.get(host, 80, ClientRequest("GET", '/robots.txt', {'Host':host}, None))
        d.addCallback(self.gotResp, 1, 309)
        d.addBoth(lastDefer.callback)
        return lastDefer
        
    def test_head(self):
        self.manager = HTTPDownloader()
        self.timeout = 10
        lastDefer = defer.Deferred()
        
        host = 'www.camrdale.org'
        d = self.manager.get(host, 80, ClientRequest("HEAD", '/robots.txt', {'Host':host}, None))
        d.addCallback(self.gotResp, 1, 0)
        d.addBoth(lastDefer.callback)
        return lastDefer
        
    def test_multiple_downloads(self):
        self.manager = HTTPDownloader()
        self.timeout = 120
        lastDefer = defer.Deferred()
        
        def newRequest(host, path, num, expect, last=False):
            d = self.manager.get(host, 80, ClientRequest("GET", path, {'Host':host}, None))
            d.addCallback(self.gotResp, num, expect)
            if last:
                d.addCallback(lastDefer.callback)
                
        newRequest('www.camrdale.org', "/", 1, 3433)
        newRequest('www.camrdale.org', "/blog/", 2, 37121)
        newRequest('www.google.ca', "/", 3, None)
        self.pending_calls.append(reactor.callLater(1, newRequest, 'www.sfu.ca', '/', 4, None))
        self.pending_calls.append(reactor.callLater(10, newRequest, 'www.camrdale.org', '/wikilink.html', 5, 3084))
        self.pending_calls.append(reactor.callLater(30, newRequest, 'www.camrdale.org', '/sitemap.html', 6, 4750))
        self.pending_calls.append(reactor.callLater(31, newRequest, 'www.sfu.ca', '/studentcentral/index.html', 7, None))
        self.pending_calls.append(reactor.callLater(32, newRequest, 'www.camrdale.org', '/openid.html', 8, 2525))
        self.pending_calls.append(reactor.callLater(32, newRequest, 'www.camrdale.org', '/subpage.html', 9, 2381))
        self.pending_calls.append(reactor.callLater(62, newRequest, 'www.google.ca', '/intl/en/options/', 0, None, True))
        return lastDefer
        
    def test_range(self):
        self.manager = HTTPDownloader()
        self.timeout = 10
        lastDefer = defer.Deferred()
        
        host = 'www.camrdale.org'
        d = self.manager.get(host, 80, ClientRequest("GET", '/robots.txt', {'Host':host, 'Range': ('bytes', [(100, 199)])}, None))
        d.addCallback(self.gotResp, 1, 100)
        d.addBoth(lastDefer.callback)
        return lastDefer
        
    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.pending_calls = []
        if self.manager:
            self.manager.closeAll()
            self.manager = None
