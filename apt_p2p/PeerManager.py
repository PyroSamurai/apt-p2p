
"""Manage a set of peers and the requests to them."""

from random import choice
from urlparse import urlparse, urlunparse
from urllib import quote_plus

from twisted.internet import reactor, defer
from twisted.python import log
from twisted.trial import unittest
from twisted.web2 import stream as stream_mod
from twisted.web2.http import splitHostPort

from HTTPDownloader import Peer
from util import uncompact

class PeerManager:
    """Manage a set of peers and the requests to them.
    
    @type clients: C{dictionary}
    @ivar clients: the available peers that have been previously contacted
    """

    def __init__(self):
        """Initialize the instance."""
        self.clients = {}
        
    def get(self, hash, mirror, peers = [], method="GET", modtime=None):
        """Download from a list of peers or fallback to a mirror.
        
        @type hash: L{Hash.HashObject}
        @param hash: the hash object containing the expected hash for the file
        @param mirror: the URI of the file on the mirror
        @type peers: C{list} of C{string}
        @param peers: a list of the peer info where the file can be found
            (optional, defaults to downloading from the mirror)
        @type method: C{string}
        @param method: the HTTP method to use, 'GET' or 'HEAD'
            (optional, defaults to 'GET')
        @type modtime: C{int}
        @param modtime: the modification time to use for an 'If-Modified-Since'
            header, as seconds since the epoch
            (optional, defaults to not sending that header)
        """
        if peers:
            # Choose one of the peers at random
            compact_peer = choice(peers)
            peer = uncompact(compact_peer['c'])
            log.msg('Downloading from peer %r' % (peer, ))
            site = peer
            path = '/~/' + quote_plus(hash.expected())
        else:
            log.msg('Downloading (%s) from mirror %s' % (method, mirror))
            parsed = urlparse(mirror)
            assert parsed[0] == "http", "Only HTTP is supported, not '%s'" % parsed[0]
            site = splitHostPort(parsed[0], parsed[1])
            path = urlunparse(('', '') + parsed[2:])

        return self.getPeer(site, path, method, modtime)
        
    def getPeer(self, site, path, method="GET", modtime=None):
        """Create a new peer if necessary and forward the request to it.
        
        @type site: (C{string}, C{int})
        @param site: the IP address and port of the peer
        @type path: C{string}
        @param path: the path to the file on the peer
        @type method: C{string}
        @param method: the HTTP method to use, 'GET' or 'HEAD'
            (optional, defaults to 'GET')
        @type modtime: C{int}
        @param modtime: the modification time to use for an 'If-Modified-Since'
            header, as seconds since the epoch
            (optional, defaults to not sending that header)
        """
        if site not in self.clients:
            self.clients[site] = Peer(site[0], site[1])
        return self.clients[site].get(path, method, modtime)
    
    def close(self):
        """Close all the connections to peers."""
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
        """Tests a normal download."""
        self.manager = PeerManager()
        self.timeout = 10
        
        host = 'www.ietf.org'
        d = self.manager.get('', 'http://' + host + '/rfc/rfc0013.txt')
        d.addCallback(self.gotResp, 1, 1070)
        return d
        
    def test_head(self):
        """Tests a 'HEAD' request."""
        self.manager = PeerManager()
        self.timeout = 10
        
        host = 'www.ietf.org'
        d = self.manager.get('', 'http://' + host + '/rfc/rfc0013.txt', method = "HEAD")
        d.addCallback(self.gotResp, 1, 0)
        return d
        
    def test_multiple_downloads(self):
        """Tests multiple downloads with queueing and connection closing."""
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