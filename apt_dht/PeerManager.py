
from random import choice
from urlparse import urlparse, urlunparse

from twisted.internet import reactor, defer
from twisted.python import log
from twisted.trial import unittest
from twisted.web2 import stream as stream_mod
from twisted.web2.http import splitHostPort

from HTTPDownloader import HTTPClientManager

class PeerManager:
    def __init__(self):
        self.clients = {}
        
    def get(self, hash, mirror, peers = [], method="GET", modtime=None):
        """Download from a list of peers or fallback to a mirror.
        
        @type peers: C{list} of C{string}
        @param peers: a list of the peers where the file can be found
        """
        if peers:
            peer = choice(peers)
            log.msg('Downloading from peer %s' % peer)
            host, port = splitHostPort('http', peer)
            path = '/~/' + hash
        else:
            log.msg('Downloading (%s) from mirror %s' % (method, mirror))
            parsed = urlparse(mirror)
            assert parsed[0] == "http", "Only HTTP is supported, not '%s'" % parsed[0]
            host, port = splitHostPort(parsed[0], parsed[1])
            path = urlunparse(('', '') + parsed[2:])

        return self.getPeer(host, port, path, method, modtime)
        
    def getPeer(self, host, port, path, method="GET", modtime=None):
        if not port:
            port = 80
        site = host + ":" + str(port)
        if site not in self.clients:
            self.clients[site] = HTTPClientManager(host, port)
        return self.clients[site].get(path, method, modtime)
    
    def close(self):
        for site in self.clients:
            self.clients[site].close()
        self.clients = {}

class TestPeerManager(unittest.TestCase):
    """Unit tests for the PeerManager."""
    
    manager = None
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
        self.manager = PeerManager()
        self.timeout = 10
        
        host = 'www.ietf.org'
        d = self.manager.get('', 'http://' + host + '/rfc/rfc0013.txt')
        d.addCallback(self.gotResp, 1, 1070)
        return d
        
    def test_head(self):
        self.manager = PeerManager()
        self.timeout = 10
        
        host = 'www.ietf.org'
        d = self.manager.get('', 'http://' + host + '/rfc/rfc0013.txt', method = "HEAD")
        d.addCallback(self.gotResp, 1, 0)
        return d
        
    def test_multiple_downloads(self):
        self.manager = PeerManager()
        self.timeout = 120
        lastDefer = defer.Deferred()
        
        def newRequest(host, path, num, expect, last=False):
            d = self.manager.get('', 'http://' + host + ':' + str(80) + path)
            d.addCallback(self.gotResp, num, expect)
            if last:
                d.addBoth(lastDefer.callback)
                
        newRequest('www.ietf.org', "/rfc/rfc0006.txt", 1, 1776)
        newRequest('www.ietf.org', "/rfc/rfc2362.txt", 2, 159833)
        newRequest('www.google.ca', "/", 3, None)
        self.pending_calls.append(reactor.callLater(1, newRequest, 'www.sfu.ca', '/', 4, None))
        self.pending_calls.append(reactor.callLater(10, newRequest, 'www.ietf.org', '/rfc/rfc0048.txt', 5, 41696))
        self.pending_calls.append(reactor.callLater(30, newRequest, 'www.ietf.org', '/rfc/rfc0022.txt', 6, 4606))
        self.pending_calls.append(reactor.callLater(31, newRequest, 'www.sfu.ca', '/studentcentral/index.html', 7, None))
        self.pending_calls.append(reactor.callLater(32, newRequest, 'www.ietf.org', '/rfc/rfc0014.txt', 8, 27))
        self.pending_calls.append(reactor.callLater(32, newRequest, 'www.ietf.org', '/rfc/rfc0001.txt', 9, 21088))
        self.pending_calls.append(reactor.callLater(62, newRequest, 'www.google.ca', '/intl/en/options/', 0, None, True))
        return lastDefer
        
    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.pending_calls = []
        if self.manager:
            self.manager.close()
            self.manager = None
