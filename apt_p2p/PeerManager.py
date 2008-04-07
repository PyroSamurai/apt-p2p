
"""Manage a set of peers and the requests to them."""

from random import choice
from urlparse import urlparse, urlunparse
from urllib import quote_plus
from binascii import b2a_hex, a2b_hex
import sha

from twisted.internet import reactor, defer
from twisted.python import log
from twisted.trial import unittest
from twisted.web2 import stream
from twisted.web2.http import splitHostPort

from HTTPDownloader import Peer
from util import uncompact
from hash import PIECE_SIZE
from apt_p2p_Khashmir.bencode import bdecode

class GrowingFileStream(stream.FileStream):
    """Modified to stream data from a file as it becomes available.
    
    @ivar CHUNK_SIZE: the maximum size of chunks of data to send at a time
    @ivar deferred: waiting for the result of the last read attempt
    @ivar available: the number of bytes that are currently available to read
    @ivar position: the current position in the file where the next read will begin
    @ivar finished: True when no more data will be coming available
    """

    CHUNK_SIZE = 4*1024

    def __init__(self, f):
        stream.FileStream.__init__(self, f)
        self.length = None
        self.deferred = None
        self.available = 0L
        self.position = 0L
        self.finished = False

    def updateAvaliable(self, newlyAvailable):
        """Update the number of bytes that are available.
        
        Call it with 0 to trigger reading of a fully read file.
        
        @param newlyAvailable: the number of bytes that just became available
        """
        assert not self.finished
        self.available += newlyAvailable
        
        # If a read is pending, let it go
        if self.deferred and self.position < self.available:
            # Try to read some data from the file
            length = self.available - self.position
            readSize = min(length, self.CHUNK_SIZE)
            self.f.seek(self.position)
            b = self.f.read(readSize)
            bytesRead = len(b)
            
            # Check if end of file was reached
            if bytesRead:
                self.position += bytesRead
                deferred = self.deferred
                self.deferred = None
                deferred.callback(b)

    def allAvailable(self):
        """Indicate that no more data is coming available."""
        self.finished = True

        # If a read is pending, let it go
        if self.deferred:
            if self.position < self.available:
                # Try to read some data from the file
                length = self.available - self.position
                readSize = min(length, self.CHUNK_SIZE)
                self.f.seek(self.position)
                b = self.f.read(readSize)
                bytesRead = len(b)
    
                # Check if end of file was reached
                if bytesRead:
                    self.position += bytesRead
                    deferred = self.deferred
                    self.deferred = None
                    deferred.callback(b)
                else:
                    # We're done
                    deferred.callback(None)
            else:
                # We're done
                deferred.callback(None)
        
    def read(self, sendfile=False):
        assert not self.deferred, "A previous read is still deferred."

        if self.f is None:
            return None

        length = self.available - self.position
        readSize = min(length, self.CHUNK_SIZE)

        # If we don't have any available, we're done or deferred
        if readSize <= 0:
            if self.finished:
                return None
            else:
                self.deferred = defer.Deferred()
                return self.deferred

        # Try to read some data from the file
        self.f.seek(self.position)
        b = self.f.read(readSize)
        bytesRead = len(b)
        if not bytesRead:
            # End of file was reached, we're done or deferred
            if self.finished:
                return None
            else:
                self.deferred = defer.Deferred()
                return self.deferred
        else:
            self.position += bytesRead
            return b

class StreamToFile(defer.Deferred):
    """Saves a stream to a file.
    
    @type stream: L{twisted.web2.stream.IByteStream}
    @ivar stream: the input stream being read
    @type outFile: L{twisted.python.filepath.FilePath}
    @ivar outFile: the file being written
    @type hash: L{Hash.HashObject}
    @ivar hash: the hash object for the file
    @type length: C{int}
    @ivar length: the length of the original (compressed) file
    @type doneDefer: L{twisted.internet.defer.Deferred}
    @ivar doneDefer: the deferred that will fire when done streaming
    """
    
    def __init__(self, inputStream, outFile, hash, start, length):
        """Initializes the file.
        
        @type inputStream: L{twisted.web2.stream.IByteStream}
        @param inputStream: the input stream to read from
        @type outFile: L{twisted.python.filepath.FilePath}
        @param outFile: the file to write to
        @type hash: L{Hash.HashObject}
        @param hash: the hash object to use for the file
        """
        self.stream = inputStream
        self.outFile = outFile
        self.hash = hash
        self.hash.new()
        self.length = self.stream.length
        self.doneDefer = None
        
    def run(self):
        """Start the streaming."""
        self.doneDefer = stream.readStream(self.stream, _gotData)
        self.doneDefer.addCallbacks(self._done, self._error)
        return self.doneDefer

    def _done(self):
        """Close all the output files, return the result."""
        if not self.outFile.closed:
            self.outFile.close()
            self.hash.digest()
            self.doneDefer.callback(self.hash)
    
    def _gotData(self, data):
        if self.outFile.closed:
            return
        
        if data is None:
            self._done()
        
        # Write and hash the streamed data
        self.outFile.write(data)
        self.hash.update(data)
        
class FileDownload:
    """Manage a download from a list of peers or a mirror.
    
    
    """
    
    def __init__(self, manager, hash, mirror, compact_peers, file):
        """Initialize the instance and check for piece hashes.
        
        @type hash: L{Hash.HashObject}
        @param hash: the hash object containing the expected hash for the file
        @param mirror: the URI of the file on the mirror
        @type compact_peers: C{list} of C{string}
        @param compact_peers: a list of the peer info where the file can be found
        @type file: L{twisted.python.filepath.FilePath}
        @param file: the temporary file to use to store the downloaded file
        """
        self.manager = manager
        self.hash = hash
        self.mirror = mirror
        self.compact_peers = compact_peers
        
        self.path = '/~/' + quote_plus(hash.expected())
        self.pieces = None
        self.started = False
        
        file.restat(False)
        if file.exists():
            file.remove()
        self.file = file.open('w')

    def run(self):
        """Start the downloading process."""
        self.defer = defer.Deferred()
        self.peers = {}
        no_pieces = 0
        pieces_string = {}
        pieces_hash = {}
        pieces_dl_hash = {}

        for compact_peer in self.compact_peers:
            # Build a list of all the peers for this download
            site = uncompact(compact_peer['c'])
            peer = manager.getPeer(site)
            self.peers.setdefault(site, {})['peer'] = peer

            # Extract any piece information from the peers list
            if 't' in compact_peer:
                self.peers[site]['t'] = compact_peer['t']['t']
                pieces_string.setdefault(compact_peer['t']['t'], 0)
                pieces_string[compact_peer['t']['t']] += 1
            elif 'h' in compact_peer:
                self.peers[site]['h'] = compact_peer['h']
                pieces_hash.setdefault(compact_peer['h'], 0)
                pieces_hash[compact_peer['h']] += 1
            elif 'l' in compact_peer:
                self.peers[site]['l'] = compact_peer['l']
                pieces_dl_hash.setdefault(compact_peer['l'], 0)
                pieces_dl_hash[compact_peer['l']] += 1
            else:
                no_pieces += 1
        
        # Select the most popular piece info
        max_found = max(no_pieces, max(pieces_string.values()),
                        max(pieces_hash.values()), max(pieces_dl_hash.values()))

        if max_found < len(self.peers):
            log.msg('Misleading piece information found, using most popular %d of %d peers' % 
                    (max_found, len(self.peers)))

        if max_found == no_pieces:
            # The file is not split into pieces
            self.pieces = []
            self.startDownload()
        elif max_found == max(pieces_string.values()):
            # Small number of pieces in a string
            for pieces, num in pieces_string.items():
                # Find the most popular piece string
                if num == max_found:
                    self.pieces = [pieces[x:x+20] for x in xrange(0, len(pieces), 20)]
                    self.startDownload()
                    break
        elif max_found == max(pieces_hash.values()):
            # Medium number of pieces stored in the DHT
            for pieces, num in pieces_hash.items():
                # Find the most popular piece hash to lookup
                if num == max_found:
                    self.getDHTPieces(pieces)
                    break
        elif max_found == max(pieces_dl_hash.values()):
            # Large number of pieces stored in peers
            for pieces, num in pieces_hash.items():
                # Find the most popular piece hash to download
                if num == max_found:
                    self.getPeerPieces(pieces)
                    break
        return self.defer

    #{ Downloading the piece hashes
    def getDHTPieces(self, key):
        """Retrieve the piece information from the DHT.
        
        @param key: the key to lookup in the DHT
        """
        # Remove any peers with the wrong piece hash
        #for site in self.peers.keys():
        #    if self.peers[site].get('h', '') != key:
        #        del self.peers[site]

        # Start the DHT lookup
        lookupDefer = self.manager.dht.getValue(key)
        lookupDefer.addCallback(self._getDHTPieces, key)
        
    def _getDHTPieces(self, results, key):
        """Check the retrieved values."""
        for result in results:
            # Make sure the hash matches the key
            result_hash = sha.new(result.get('t', '')).digest()
            if result_hash == key:
                pieces = result['t']
                self.pieces = [pieces[x:x+20] for x in xrange(0, len(pieces), 20)]
                log.msg('Retrieved %d piece hashes from the DHT' % len(self.pieces))
                self.startDownload()
                return
            
        # Continue without the piece hashes
        log.msg('Could not retrieve the piece hashes from the DHT')
        self.pieces = []
        self.startDownload()

    def getPeerPieces(self, key, failedSite = None):
        """Retrieve the piece information from the peers.
        
        @param key: the key to request from the peers
        """
        if failedSite is None:
            self.outstanding = 0
            # Remove any peers with the wrong piece hash
            #for site in self.peers.keys():
            #    if self.peers[site].get('l', '') != key:
            #        del self.peers[site]
        else:
            self.peers[failedSite]['failed'] = True
            self.outstanding -= 1

        if self.pieces is None:
            # Send a request to one or more peers
            for site in self.peers:
                if self.peers[site].get('failed', False) != True:
                    path = '/~/' + quote_plus(key)
                    lookupDefer = self.peers[site]['peer'].get(path)
                    lookupDefer.addCallbacks(self._getPeerPieces, self._gotPeerError,
                                             callbackArgs=(key, site), errbackArgs=(key, site))
                    self.outstanding += 1
                    if self.outstanding >= 3:
                        break
        
        if self.pieces is None and self.outstanding == 0:
            # Continue without the piece hashes
            log.msg('Could not retrieve the piece hashes from the peers')
            self.pieces = []
            self.startDownload()
        
    def _getPeerPieces(self, response, key, site):
        """Process the retrieved response from the peer."""
        if response.code != 200:
            # Request failed, try a different peer
            self.getPeerPieces(key, site)
        else:
            # Read the response stream to a string
            self.peers[site]['pieces'] = ''
            def _gotPeerPiece(data, self = self, site = site):
                self.peers[site]['pieces'] += data
            df = stream.readStream(response.stream, _gotPeerPiece)
            df.addCallbacks(self._gotPeerPieces, self._gotPeerError,
                            callbackArgs=(key, site), errbackArgs=(key, site))

    def _gotPeerError(self, err, key, site):
        """Peer failed, try again."""
        log.err(err)
        self.getPeerPieces(key, site)

    def _gotPeerPieces(self, result, key, site):
        """Check the retrieved pieces from the peer."""
        if self.pieces is not None:
            # Already done
            return
        
        try:
            result = bdecode(self.peers[site]['pieces'])
        except:
            log.err()
            self.getPeerPieces(key, site)
            return
            
        result_hash = sha.new(result.get('t', '')).digest()
        if result_hash == key:
            pieces = result['t']
            self.pieces = [pieces[x:x+20] for x in xrange(0, len(pieces), 20)]
            log.msg('Retrieved %d piece hashes from the peer' % len(self.pieces))
            self.startDownload()
        else:
            log.msg('Peer returned a piece string that did not match')
            self.getPeerPieces(key, site)

    #{ Downloading the file
    def sort(self):
        """Sort the peers by their rank (highest ranked at the end)."""
        def sort(a, b):
            """Sort peers by their rank."""
            if a.rank > b.rank:
                return 1
            elif a.rank < b.rank:
                return -1
            return 0
        self.peerlist.sort(sort)

    def startDownload(self):
        """Start the download from the peers."""
        # Don't start twice
        if self.started:
            return
        
        self.started = True
        assert self.pieces is not None, "You must initialize the piece hashes first"
        self.peerlist = [self.peers[site]['peer'] for site in self.peers]
        
        # Special case if there's only one good peer left
        if len(self.peerlist) == 1:
            log.msg('Downloading from peer %r' % (self.peerlist[0], ))
            self.defer.callback(self.peerlist[0].get(self.path))
            return
        
        self.sort()
        self.outstanding = 0
        self.next_piece = 0
        
        while self.outstanding < 3 and self.peerlist and self.next_piece < len(self.pieces):
            peer = self.peerlist.pop()
            piece = self.next_piece
            self.next_piece += 1
            
            self.outstanding += 1
            df = peer.getRange(self.path, piece*PIECE_SIZE, (piece+1)*PIECE_SIZE - 1)
            df.addCallbacks(self._gotPiece, self._gotError,
                            callbackArgs=(piece, peer), errbackArgs=(piece, peer))
    
    def _gotPiece(self, response, piece, peer):
        """Process the retrieved piece from the peer."""
        if response.code != 206:
            # Request failed, try a different peer
            self.getPeerPieces(key, site)
        else:
            # Read the response stream to the file
            df = StreamToFile(response.stream, self.file, self.hash, piece*PIECE_SIZE, PIECE_SIZE).run()
            df.addCallbacks(self._gotPeerPieces, self._gotPeerError,
                            callbackArgs=(key, site), errbackArgs=(key, site))

    def _gotError(self, err, piece, peer):
        """Peer failed, try again."""
        log.err(err)

        
class PeerManager:
    """Manage a set of peers and the requests to them.
    
    @type clients: C{dictionary}
    @ivar clients: the available peers that have been previously contacted
    """

    def __init__(self, cache_dir, dht):
        """Initialize the instance."""
        self.cache_dir = cache_dir
        self.cache_dir.restat(False)
        if not self.cache_dir.exists():
            self.cache_dir.makedirs()
        self.dht = dht
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
        if not peers or method != "GET" or modtime is not None:
            log.msg('Downloading (%s) from mirror %s' % (method, mirror))
            parsed = urlparse(mirror)
            assert parsed[0] == "http", "Only HTTP is supported, not '%s'" % parsed[0]
            site = splitHostPort(parsed[0], parsed[1])
            path = urlunparse(('', '') + parsed[2:])
            peer = self.getPeer(site)
            return peer.get(path, method, modtime)
        elif len(peers) == 1:
            site = uncompact(peers[0]['c'])
            log.msg('Downloading from peer %r' % (site, ))
            path = '/~/' + quote_plus(hash.expected())
            peer = self.getPeer(site)
            return peer.get(path)
        else:
            tmpfile = self.cache_dir.child(hash.hexexpected())
            return FileDownload(self, hash, mirror, peers, tmpfile).run()
        
    def getPeer(self, site):
        """Create a new peer if necessary and return it.
        
        @type site: (C{string}, C{int})
        @param site: the IP address and port of the peer
        """
        if site not in self.clients:
            self.clients[site] = Peer(site[0], site[1])
        return self.clients[site]
    
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
        stream.readStream(resp.stream, print_).addCallback(printdone)
    
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
