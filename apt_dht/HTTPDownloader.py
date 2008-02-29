
from math import exp
from datetime import datetime, timedelta

from twisted.internet import reactor, defer, protocol
from twisted.internet.protocol import ClientFactory
from twisted import version as twisted_version
from twisted.python import log
from twisted.web2.client.interfaces import IHTTPClientManager
from twisted.web2.client.http import ProtocolError, ClientRequest, HTTPClientProtocol
from twisted.web2 import stream as stream_mod, http_headers
from twisted.web2 import version as web2_version
from twisted.trial import unittest
from zope.interface import implements

from apt_dht_conf import version

class Peer(ClientFactory):
    """A manager for all HTTP requests to a single peer.
    
    Controls all requests that go to a single peer (host and port).
    This includes buffering requests until they can be sent and reconnecting
    in the event of the connection being closed.
    
    """

    implements(IHTTPClientManager)
    
    def __init__(self, host, port=80):
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
        self._errors = 0
        self._completed = 0
        self._downloadSpeeds = []
        self._lastResponse = None
        self._responseTimes = []
        
    def connect(self):
        assert self.closed and not self.connecting
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

    def submitRequest(self, request):
        request.submissionTime = datetime.now()
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
        req.deferResponse.addCallbacks(self.requestComplete, self.requestError)

    def requestComplete(self, resp):
        self._processLastResponse()
        req = self.response_queue.pop(0)
        log.msg('%s of %s completed with code %d' % (req.method, req.uri, resp.code))
        self._completed += 1
        if resp.code >= 400:
            self._errors += 1
        now = datetime.now()
        self._responseTimes.append((now, now - req.submissionTime))
        self._lastResponse = (now, resp.stream.length)
        req.deferRequest.callback(resp)

    def requestError(self, error):
        self._processLastResponse()
        req = self.response_queue.pop(0)
        log.msg('Download of %s generated error %r' % (req.uri, error))
        self._completed += 1
        self._errors += 1
        req.deferRequest.errback(error)
        
    def hashError(self, error):
        """Log that a hash error occurred from the peer."""
        log.msg('Hash error from peer (%s, %d): %r' % (self.host, self.port, error))
        self._errors += 1

    # The IHTTPClientManager interface functions
    def clientBusy(self, proto):
        self.busy = True

    def clientIdle(self, proto):
        self._processLastResponse()
        self.busy = False
        self.processQueue()

    def clientPipelining(self, proto):
        self.pipeline = True
        self.processQueue()

    def clientGone(self, proto):
        self._processLastResponse()
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
            
    # The downloading request interface functions
    def setCommonHeaders(self):
        headers = http_headers.Headers()
        headers.setHeader('Host', self.host)
        headers.setHeader('User-Agent', 'apt-dht/%s (twisted/%s twisted.web2/%s)' % 
                          (version.short(), twisted_version.short(), web2_version.short()))
        return headers
    
    def get(self, path, method="GET", modtime=None):
        headers = self.setCommonHeaders()
        if modtime:
            headers.setHeader('If-Modified-Since', modtime)
        return self.submitRequest(ClientRequest(method, path, headers, None))
    
    def getRange(self, path, rangeStart, rangeEnd, method="GET"):
        headers = self.setCommonHeaders()
        headers.setHeader('Range', ('bytes', [(rangeStart, rangeEnd)]))
        return self.submitRequest(ClientRequest(method, path, headers, None))
    
    # Functions that return information about the peer
    def isIdle(self):
        return not self.busy and not self.request_queue and not self.response_queue
    
    def _processLastResponse(self):
        if self._lastResponse is not None:
            now = datetime.now()
            self._downloadSpeeds.append((now, now - self._lastResponse[0], self._lastResponse[1]))
            self._lastResponse = None
            
    def downloadSpeed(self):
        """Gets the latest average download speed for the peer.
        
        The average is over the last 10 responses that occurred in the last hour.
        """
        total_time = 0.0
        total_download = 0
        now = datetime.now()
        while self._downloadSpeeds and (len(self._downloadSpeeds) > 10 or 
                                        now - self._downloadSpeeds[0][0] > timedelta(seconds=3600)):
            self._downloadSpeeds.pop(0)

        # If there are none, then you get 0
        if not self._downloadSpeeds:
            return 0.0
        
        for download in self._downloadSpeeds:
            total_time += download[1].days*86400.0 + download[1].seconds + download[1].microseconds/1000000.0
            total_download += download[2]

        return total_download / total_time
    
    def responseTime(self):
        """Gets the latest average response time for the peer.
        
        Response time is the time from receiving the request, to the time
        the download begins. The average is over the last 10 responses that
        occurred in the last hour.
        """
        total_response = 0.0
        now = datetime.now()
        while self._responseTimes and (len(self._responseTimes) > 10 or 
                                       now - self._responseTimes[0][0] > timedelta(seconds=3600)):
            self._responseTimes.pop(0)

        # If there are none, give it the benefit of the doubt
        if not self._responseTimes:
            return 0.0

        for response in self._responseTimes:
            total_response += response[1].days*86400.0 + response[1].seconds + response[1].microseconds/1000000.0

        return total_response / len(self._responseTimes)
    
    def rank(self, fastest):
        """Determine the ranking value for the peer.
        
        The ranking value is composed of 5 numbers:
         - 1 if a connection to the peer is open, 0.9 otherwise
         - 1 if there are no pending requests, to 0 if there are a maximum
         - 1 if the peer is the fastest of all peers, to 0 if the speed is 0
         - 1 if all requests are good, 0 if all produced errors
         - an exponentially decreasing number based on the response time
        """
        rank = 1.0
        if self.closed:
            rank *= 0.9
        rank *= (max(0.0, 10.0 - len(self.request_queue) - len(self.response_queue))) / 10.0
        if fastest > 0.0:
            rank *= min(1.0, self.downloadSpeed() / fastest)
        if self._completed:
            rank *= max(0.0, 1.0 - float(self._errors) / self._completed)
        rank *= exp(-self.responseTime() / 5.0)
        return rank
        
class TestClientManager(unittest.TestCase):
    """Unit tests for the Peer."""
    
    client = None
    pending_calls = []
    
    def gotResp(self, resp, num, expect):
        self.failUnless(resp.code >= 200 and resp.code < 300, "Got a non-200 response: %r" % resp.code)
        if expect is not None:
            self.failUnless(resp.stream.length == expect, "Length was incorrect, got %r, expected %r" % (resp.stream.length, expect))
        def print_(n):
            pass
        def printdone(n):
            pass
        stream_mod.readStream(resp.stream, print_).addCallback(printdone)
    
    def test_download(self):
        host = 'www.ietf.org'
        self.client = Peer(host, 80)
        self.timeout = 10
        
        d = self.client.get('/rfc/rfc0013.txt')
        d.addCallback(self.gotResp, 1, 1070)
        return d
        
    def test_head(self):
        host = 'www.ietf.org'
        self.client = Peer(host, 80)
        self.timeout = 10
        
        d = self.client.get('/rfc/rfc0013.txt', "HEAD")
        d.addCallback(self.gotResp, 1, 0)
        return d
        
    def test_multiple_downloads(self):
        host = 'www.ietf.org'
        self.client = Peer(host, 80)
        self.timeout = 120
        lastDefer = defer.Deferred()
        
        def newRequest(path, num, expect, last=False):
            d = self.client.get(path)
            d.addCallback(self.gotResp, num, expect)
            if last:
                d.addBoth(lastDefer.callback)
                
        newRequest("/rfc/rfc0006.txt", 1, 1776)
        newRequest("/rfc/rfc2362.txt", 2, 159833)
        newRequest("/rfc/rfc0801.txt", 3, 40824)
        self.pending_calls.append(reactor.callLater(1, newRequest, '/rfc/rfc0013.txt', 4, 1070))
        self.pending_calls.append(reactor.callLater(10, newRequest, '/rfc/rfc0022.txt', 5, 4606))
        self.pending_calls.append(reactor.callLater(30, newRequest, '/rfc/rfc0048.txt', 6, 41696))
        self.pending_calls.append(reactor.callLater(31, newRequest, '/rfc/rfc3261.txt', 7, 647976))
        self.pending_calls.append(reactor.callLater(32, newRequest, '/rfc/rfc0014.txt', 8, 27))
        self.pending_calls.append(reactor.callLater(32, newRequest, '/rfc/rfc0001.txt', 9, 21088))
        self.pending_calls.append(reactor.callLater(62, newRequest, '/rfc/rfc2801.txt', 0, 598794, True))
        return lastDefer
        
    def test_multiple_quick_downloads(self):
        host = 'www.ietf.org'
        self.client = Peer(host, 80)
        self.timeout = 30
        lastDefer = defer.Deferred()
        
        def newRequest(path, num, expect, last=False):
            d = self.client.get(path)
            d.addCallback(self.gotResp, num, expect)
            if last:
                d.addBoth(lastDefer.callback)
                
        newRequest("/rfc/rfc0006.txt", 1, 1776)
        newRequest("/rfc/rfc2362.txt", 2, 159833)
        newRequest("/rfc/rfc0801.txt", 3, 40824)
        self.pending_calls.append(reactor.callLater(0, newRequest, '/rfc/rfc0013.txt', 4, 1070))
        self.pending_calls.append(reactor.callLater(0, newRequest, '/rfc/rfc0022.txt', 5, 4606))
        self.pending_calls.append(reactor.callLater(0, newRequest, '/rfc/rfc0048.txt', 6, 41696))
        self.pending_calls.append(reactor.callLater(0, newRequest, '/rfc/rfc3261.txt', 7, 647976))
        self.pending_calls.append(reactor.callLater(0, newRequest, '/rfc/rfc0014.txt', 8, 27))
        self.pending_calls.append(reactor.callLater(0, newRequest, '/rfc/rfc0001.txt', 9, 21088))
        self.pending_calls.append(reactor.callLater(0, newRequest, '/rfc/rfc2801.txt', 0, 598794, True))
        return lastDefer
        
    def checkInfo(self):
        log.msg('Rank is: %r' % self.client.rank(250.0*1024))
        log.msg('Download speed is: %r' % self.client.downloadSpeed())
        log.msg('Response Time is: %r' % self.client.responseTime())
        
    def test_peer_info(self):
        host = 'www.ietf.org'
        self.client = Peer(host, 80)
        self.timeout = 120
        lastDefer = defer.Deferred()
        
        def newRequest(path, num, expect, last=False):
            d = self.client.get(path)
            d.addCallback(self.gotResp, num, expect)
            if last:
                d.addBoth(lastDefer.callback)
                
        newRequest("/rfc/rfc0006.txt", 1, 1776)
        newRequest("/rfc/rfc2362.txt", 2, 159833)
        newRequest("/rfc/rfc0801.txt", 3, 40824)
        self.pending_calls.append(reactor.callLater(1, newRequest, '/rfc/rfc0013.txt', 4, 1070))
        self.pending_calls.append(reactor.callLater(10, newRequest, '/rfc/rfc0022.txt', 5, 4606))
        self.pending_calls.append(reactor.callLater(30, newRequest, '/rfc/rfc0048.txt', 6, 41696))
        self.pending_calls.append(reactor.callLater(31, newRequest, '/rfc/rfc3261.txt', 7, 647976))
        self.pending_calls.append(reactor.callLater(32, newRequest, '/rfc/rfc0014.txt', 8, 27))
        self.pending_calls.append(reactor.callLater(32, newRequest, '/rfc/rfc0001.txt', 9, 21088))
        self.pending_calls.append(reactor.callLater(62, newRequest, '/rfc/rfc2801.txt', 0, 598794, True))
        
        for i in xrange(2, 122, 2):
            self.pending_calls.append(reactor.callLater(i, self.checkInfo))
        
        return lastDefer
        
    def test_range(self):
        host = 'www.ietf.org'
        self.client = Peer(host, 80)
        self.timeout = 10
        
        d = self.client.getRange('/rfc/rfc0013.txt', 100, 199)
        d.addCallback(self.gotResp, 1, 100)
        return d
        
    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.pending_calls = []
        if self.client:
            self.client.close()
            self.client = None
