
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
        
    def get(self, locations, method="GET", modtime=None):
        """Download from a list of peers.
        
        @type locations: C{list} of C{string}
        @var locations: a list of the locations where the file can be found
        """
        url = choice(locations)
        log.msg('Downloading %s' % url)
        parsed = urlparse(url)
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
        
        host = 'www.camrdale.org'
        d = self.manager.get(['http://' + host + '/robots.txt'])
        d.addCallback(self.gotResp, 1, 309)
        return d
        
    def test_head(self):
        self.manager = PeerManager()
        self.timeout = 10
        
        host = 'www.camrdale.org'
        d = self.manager.get(['http://' + host + '/robots.txt'], "HEAD")
        d.addCallback(self.gotResp, 1, 0)
        return d
        
    def test_multiple_downloads(self):
        self.manager = PeerManager()
        self.timeout = 120
        lastDefer = defer.Deferred()
        
        def newRequest(host, path, num, expect, last=False):
            d = self.manager.get(['http://' + host + ':' + str(80) + path])
            d.addCallback(self.gotResp, num, expect)
            if last:
                d.addBoth(lastDefer.callback)
                
        newRequest('www.camrdale.org', "/", 1, 3433)
        newRequest('www.camrdale.org', "/blog/", 2, 39152)
        newRequest('www.google.ca', "/", 3, None)
        self.pending_calls.append(reactor.callLater(1, newRequest, 'www.sfu.ca', '/', 4, None))
        self.pending_calls.append(reactor.callLater(10, newRequest, 'www.camrdale.org', '/wikilink.html', 5, 3084))
        self.pending_calls.append(reactor.callLater(30, newRequest, 'www.camrdale.org', '/sitemap.html', 6, 4756))
        self.pending_calls.append(reactor.callLater(31, newRequest, 'www.sfu.ca', '/studentcentral/index.html', 7, None))
        self.pending_calls.append(reactor.callLater(32, newRequest, 'www.camrdale.org', '/openid.html', 8, 2525))
        self.pending_calls.append(reactor.callLater(32, newRequest, 'www.camrdale.org', '/subpage.html', 9, 2381))
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
