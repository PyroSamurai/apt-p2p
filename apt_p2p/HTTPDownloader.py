
"""Manage all download requests to a single site."""

from math import exp
from datetime import datetime, timedelta

from twisted.internet import reactor, defer, protocol
from twisted.internet.protocol import ClientFactory
from twisted import version as twisted_version
from twisted.python import log
from twisted.web2.client.interfaces import IHTTPClientManager
from twisted.web2.client.http import ProtocolError, ClientRequest, HTTPClientProtocol, HTTPClientChannelRequest
from twisted.web2.channel.http import PERSIST_NO_PIPELINE, PERSIST_PIPELINE
from twisted.web2 import stream as stream_mod, http_headers
from twisted.web2 import version as web2_version
from twisted.trial import unittest
from zope.interface import implements

from apt_p2p_conf import version

class PipelineError(Exception):
    """An error has occurred in pipelining requests."""

class FixedHTTPClientChannelRequest(HTTPClientChannelRequest):
    """Fix the broken _error function."""

    def __init__(self, channel, request, closeAfter):
        HTTPClientChannelRequest.__init__(self, channel, request, closeAfter)
        self.started = False

    def _error(self, err):
        """
        Abort parsing, and depending of the status of the request, either fire
        the C{responseDefer} if no response has been sent yet, or close the
        stream.
        """
        if self.started:
            self.abortParse()
        if hasattr(self, 'stream') and self.stream is not None:
            self.stream.finish(err)
        else:
            self.responseDefer.errback(err)

    def gotInitialLine(self, initialLine):
        self.started = True
        HTTPClientChannelRequest.gotInitialLine(self, initialLine)
    
class LoggingHTTPClientProtocol(HTTPClientProtocol):
    """A modified client protocol that logs the number of bytes received."""
    
    def __init__(self, factory, stats = None, mirror = False):
        HTTPClientProtocol.__init__(self, factory)
        self.stats = stats
        self.mirror = mirror
    
    def lineReceived(self, line):
        if self.stats:
            self.stats.receivedBytes(len(line) + 2, self.mirror)
        HTTPClientProtocol.lineReceived(self, line)

    def rawDataReceived(self, data):
        if self.stats:
            self.stats.receivedBytes(len(data), self.mirror)
        HTTPClientProtocol.rawDataReceived(self, data)

    def submitRequest(self, request, closeAfter=True):
        """
        @param request: The request to send to a remote server.
        @type request: L{ClientRequest}

        @param closeAfter: If True the 'Connection: close' header will be sent,
            otherwise 'Connection: keep-alive'
        @type closeAfter: C{bool}

        @rtype: L{twisted.internet.defer.Deferred}
        @return: A Deferred which will be called back with the
            L{twisted.web2.http.Response} from the server.
        """

        # Assert we're in a valid state to submit more
        assert self.outRequest is None
        assert ((self.readPersistent is PERSIST_NO_PIPELINE
                 and not self.inRequests)
                or self.readPersistent is PERSIST_PIPELINE)

        self.manager.clientBusy(self)
        if closeAfter:
            self.readPersistent = False

        self.outRequest = chanRequest = FixedHTTPClientChannelRequest(self,
                                            request, closeAfter)
        self.inRequests.append(chanRequest)

        chanRequest.submit()
        return chanRequest.responseDefer

    def setReadPersistent(self, persist):
        oldPersist = self.readPersistent
        self.readPersistent = persist
        if not persist:
            # Tell all requests but first to abort.
            lostRequests = self.inRequests[1:]
            del self.inRequests[1:]
            for request in lostRequests:
                request.connectionLost(PipelineError('Pipelined connection was closed.'))
        elif (oldPersist is PERSIST_NO_PIPELINE and
              persist is PERSIST_PIPELINE and
              self.outRequest is None):
            self.manager.clientPipelining(self)

    def connectionLost(self, reason):
        self.readPersistent = False
        self.setTimeout(None)
        self.manager.clientGone(self)
        # Cancel the current request
        if self.inRequests and self.inRequests[0] is not None:
            self.inRequests[0].connectionLost(reason)
        # Tell all remaining requests to abort.
        lostRequests = self.inRequests[1:]
        del self.inRequests[1:]
        for request in lostRequests:
            if request is not None:
                request.connectionLost(PipelineError('Pipelined connection was closed.'))
                
class Peer(ClientFactory):
    """A manager for all HTTP requests to a single peer.
    
    Controls all requests that go to a single peer (host and port).
    This includes buffering requests until they can be sent and reconnecting
    in the event of the connection being closed.
    
    """

    implements(IHTTPClientManager)
    
    def __init__(self, host, port = 80, stats = None):
        self.host = host
        self.port = port
        self.stats = stats
        self.mirror = False
        self.rank = 0.01
        self.busy = False
        self.pipeline = False
        self.closed = True
        self.connecting = False
        self.request_queue = []
        self.outstanding = 0
        self.proto = None
        self.connector = None
        self._errors = 0
        self._completed = 0
        self._downloadSpeeds = []
        self._lastResponse = None
        self._responseTimes = []
    
    def __repr__(self):
        return "(%r, %r, %r)" % (self.host, self.port, self.rank)
        
    #{ Manage the request queue
    def connect(self):
        """Connect to the peer."""
        assert self.closed and not self.connecting
        log.msg('Connecting to (%s, %d)' % (self.host, self.port))
        self.connecting = True
        d = protocol.ClientCreator(reactor, LoggingHTTPClientProtocol, self,
                                   stats = self.stats, mirror = self.mirror).connectTCP(self.host, self.port)
        d.addCallbacks(self.connected, self.connectionError)

    def connected(self, proto):
        """Begin processing the queued requests."""
        log.msg('Connected to (%s, %d)' % (self.host, self.port))
        self.closed = False
        self.connecting = False
        self.proto = proto
        reactor.callLater(0, self.processQueue)
        
    def connectionError(self, err):
        """Cancel the requests."""
        log.msg('Failed to connect to the peer by HTTP.')
        log.err(err)

        # Remove one request so that we don't loop indefinitely
        if self.request_queue:
            req, deferRequest, submissionTime = self.request_queue.pop(0)
            deferRequest.errback(err)
            
        self._completed += 1
        self._errors += 1
        self.rerank()
        if self.connecting:
            self.connecting = False
            self.clientGone(None)
        
    def close(self):
        """Close the connection to the peer."""
        if not self.closed:
            self.proto.transport.loseConnection()

    def submitRequest(self, request):
        """Add a new request to the queue.
        
        @type request: L{twisted.web2.client.http.ClientRequest}
        @return: deferred that will fire with the completed request
        """
        submissionTime = datetime.now()
        deferRequest = defer.Deferred()
        self.request_queue.append((request, deferRequest, submissionTime))
        self.rerank()
        reactor.callLater(0, self.processQueue)
        return deferRequest

    def processQueue(self):
        """Check the queue to see if new requests can be sent to the peer."""
        if not self.request_queue:
            return
        if self.connecting:
            return
        if self.closed:
            self.connect()
            return
        if self.busy and not self.pipeline:
            return
        if self.outstanding and not self.pipeline:
            return
        if not ((self.proto.readPersistent is PERSIST_NO_PIPELINE
                 and not self.proto.inRequests)
                 or self.proto.readPersistent is PERSIST_PIPELINE):
            log.msg('HTTP protocol is not ready though we were told to pipeline: %r, %r' %
                    (self.proto.readPersistent, self.proto.inRequests))
            return

        req, deferRequest, submissionTime = self.request_queue.pop(0)
        try:
            deferResponse = self.proto.submitRequest(req, False)
        except:
            # Try again later
            log.msg('Got an error trying to submit a new HTTP request %s' % (request.uri, ))
            log.err()
            self.request_queue.insert(0, (request, deferRequest, submissionTime))
            ractor.callLater(1, self.processQueue)
            return
            
        self.outstanding += 1
        self.rerank()
        deferResponse.addCallbacks(self.requestComplete, self.requestError,
                                   callbackArgs = (req, deferRequest, submissionTime),
                                   errbackArgs = (req, deferRequest))

    def requestComplete(self, resp, req, deferRequest, submissionTime):
        """Process a completed request."""
        self._processLastResponse()
        self.outstanding -= 1
        assert self.outstanding >= 0
        log.msg('%s of %s completed with code %d (%r)' % (req.method, req.uri, resp.code, resp.headers))
        self._completed += 1
        now = datetime.now()
        self._responseTimes.append((now, now - submissionTime))
        self._lastResponse = (now, resp.stream.length)
        self.rerank()
        deferRequest.callback(resp)

    def requestError(self, error, req, deferRequest):
        """Process a request that ended with an error."""
        self._processLastResponse()
        self.outstanding -= 1
        assert self.outstanding >= 0
        log.msg('Download of %s generated error %r' % (req.uri, error))
        self._completed += 1
        self._errors += 1
        self.rerank()
        deferRequest.errback(error)
        
    def hashError(self, error):
        """Log that a hash error occurred from the peer."""
        log.msg('Hash error from peer (%s, %d): %r' % (self.host, self.port, error))
        self._errors += 1
        self.rerank()

    #{ IHTTPClientManager interface
    def clientBusy(self, proto):
        """Save the busy state."""
        self.busy = True

    def clientIdle(self, proto):
        """Try to send a new request."""
        self._processLastResponse()
        self.busy = False
        reactor.callLater(0, self.processQueue)
        self.rerank()

    def clientPipelining(self, proto):
        """Try to send a new request."""
        self.pipeline = True
        reactor.callLater(0, self.processQueue)

    def clientGone(self, proto):
        """Mark sent requests as errors."""
        self._processLastResponse()
        log.msg('Lost the connection to (%s, %d)' % (self.host, self.port))
        self.busy = False
        self.pipeline = False
        self.closed = True
        self.connecting = False
        self.proto = None
        self.rerank()
        if self.request_queue:
            reactor.callLater(0, self.processQueue)
            
    #{ Downloading request interface
    def setCommonHeaders(self):
        """Get the common HTTP headers for all requests."""
        headers = http_headers.Headers()
        headers.setHeader('Host', self.host)
        headers.setHeader('User-Agent', 'apt-p2p/%s (twisted/%s twisted.web2/%s)' % 
                          (version.short(), twisted_version.short(), web2_version.short()))
        return headers
    
    def get(self, path, method="GET", modtime=None):
        """Add a new request to the queue.
        
        @type path: C{string}
        @param path: the path to request from the peer
        @type method: C{string}
        @param method: the HTTP method to use, 'GET' or 'HEAD'
            (optional, defaults to 'GET')
        @type modtime: C{int}
        @param modtime: the modification time to use for an 'If-Modified-Since'
            header, as seconds since the epoch
            (optional, defaults to not sending that header)
        """
        headers = self.setCommonHeaders()
        if modtime:
            headers.setHeader('If-Modified-Since', modtime)
        return self.submitRequest(ClientRequest(method, path, headers, None))
    
    def getRange(self, path, rangeStart, rangeEnd, method="GET"):
        """Add a new request with a Range header to the queue.
        
        @type path: C{string}
        @param path: the path to request from the peer
        @type rangeStart: C{int}
        @param rangeStart: the byte to begin the request at
        @type rangeEnd: C{int}
        @param rangeEnd: the byte to end the request at (inclusive)
        @type method: C{string}
        @param method: the HTTP method to use, 'GET' or 'HEAD'
            (optional, defaults to 'GET')
        """
        headers = self.setCommonHeaders()
        headers.setHeader('Range', ('bytes', [(rangeStart, rangeEnd)]))
        return self.submitRequest(ClientRequest(method, path, headers, None))
    
    #{ Peer information
    def isIdle(self):
        """Check whether the peer is idle or not."""
        return not self.busy and not self.request_queue and not self.outstanding
    
    def _processLastResponse(self):
        """Save the download time of the last request for speed calculations."""
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
    
    def rerank(self):
        """Determine the ranking value for the peer.
        
        The ranking value is composed of 5 numbers, each exponentially
        decreasing from 1 to 0 based on:
         - if a connection to the peer is open
         - the number of pending requests
         - the time to download a single piece
         - the number of errors
         - the response time
        """
        rank = 1.0
        if self.closed:
            rank *= 0.9
        rank *= exp(-(len(self.request_queue) + self.outstanding))
        speed = self.downloadSpeed()
        if speed > 0.0:
            rank *= exp(-512.0*1024 / speed)
        if self._completed:
            rank *= exp(-10.0 * self._errors / self._completed)
        rank *= exp(-self.responseTime() / 5.0)
        self.rank = rank
        
class TestClientManager(unittest.TestCase):
    """Unit tests for the Peer."""
    
    client = None
    pending_calls = []
    length = []
    
    def gotResp(self, resp, num, expect):
        self.failUnless(resp.code >= 200 and resp.code < 300, "Got a non-200 response: %r" % resp.code)
        if expect is not None:
            self.failUnless(resp.stream.length == expect, "Length was incorrect, got %r, expected %r" % (resp.stream.length, expect))
        while len(self.length) <= num:
            self.length.append(0)
        self.length[num] = 0
        def addData(data, self = self, num = num):
            self.length[num] += len(data)
        def checkLength(resp, self = self, num = num, length = resp.stream.length):
            self.failUnlessEqual(self.length[num], length)
            return resp
        df = stream_mod.readStream(resp.stream, addData)
        df.addCallback(checkLength)
        return df
    
    def test_download(self):
        """Tests a normal download."""
        host = 'www.ietf.org'
        self.client = Peer(host, 80)
        self.timeout = 10
        
        d = self.client.get('/rfc/rfc0013.txt')
        d.addCallback(self.gotResp, 1, 1070)
        return d
        
    def test_head(self):
        """Tests a 'HEAD' request."""
        host = 'www.ietf.org'
        self.client = Peer(host, 80)
        self.timeout = 10
        
        d = self.client.get('/rfc/rfc0013.txt', "HEAD")
        d.addCallback(self.gotResp, 1, 0)
        return d
        
    def test_multiple_downloads(self):
        """Tests multiple downloads with queueing and connection closing."""
        host = 'www.ietf.org'
        self.client = Peer(host, 80)
        self.timeout = 120
        lastDefer = defer.Deferred()
        
        def newRequest(path, num, expect, last=False):
            d = self.client.get(path)
            d.addCallback(self.gotResp, num, expect)
            if last:
                d.addBoth(lastDefer.callback)

        # 3 quick requests
        newRequest("/rfc/rfc0006.txt", 1, 1776)
        newRequest("/rfc/rfc2362.txt", 2, 159833)
        newRequest("/rfc/rfc0801.txt", 3, 40824)
        
        # This one will probably be queued
        self.pending_calls.append(reactor.callLater(6, newRequest, '/rfc/rfc0013.txt', 4, 1070))
        
        # Connection should still be open, but idle
        self.pending_calls.append(reactor.callLater(10, newRequest, '/rfc/rfc0022.txt', 5, 4606))
        
        #Connection should be closed
        self.pending_calls.append(reactor.callLater(30, newRequest, '/rfc/rfc0048.txt', 6, 41696))
        self.pending_calls.append(reactor.callLater(31, newRequest, '/rfc/rfc3261.txt', 7, 647976))
        self.pending_calls.append(reactor.callLater(32, newRequest, '/rfc/rfc0014.txt', 8, 27))
        self.pending_calls.append(reactor.callLater(32, newRequest, '/rfc/rfc0001.txt', 9, 21088))
        
        # Now it should definitely be closed
        self.pending_calls.append(reactor.callLater(62, newRequest, '/rfc/rfc2801.txt', 0, 598794, True))
        return lastDefer
        
    def test_multiple_quick_downloads(self):
        """Tests lots of multiple downloads with queueing."""
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
        log.msg('Rank is: %r' % self.client.rank)
        log.msg('Download speed is: %r' % self.client.downloadSpeed())
        log.msg('Response Time is: %r' % self.client.responseTime())
        
    def test_peer_info(self):
        """Test retrieving the peer info during a download."""
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
        """Test a Range request."""
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
