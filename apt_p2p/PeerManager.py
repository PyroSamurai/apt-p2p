
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
from twisted.web2.http import Response, splitHostPort

from HTTPDownloader import Peer
from util import uncompact
from Hash import PIECE_SIZE
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

    def __init__(self, f, length = None):
        stream.FileStream.__init__(self, f)
        self.length = length
        self.deferred = None
        self.available = 0L
        self.position = 0L
        self.finished = False

    def updateAvailable(self, newlyAvailable):
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
        """Indicate that no more data will be coming available."""
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
                    deferred = self.deferred
                    self.deferred = None
                    deferred.callback(None)
            else:
                # We're done
                deferred = self.deferred
                self.deferred = None
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

class StreamToFile:
    """Save a stream to a partial file and hash it.
    
    @type stream: L{twisted.web2.stream.IByteStream}
    @ivar stream: the input stream being read
    @type outFile: L{twisted.python.filepath.FilePath}
    @ivar outFile: the file being written
    @type hash: C{sha1}
    @ivar hash: the hash object for the data
    @type position: C{int}
    @ivar position: the current file position to write the next data to
    @type length: C{int}
    @ivar length: the position in the file to not write beyond
    @type doneDefer: L{twisted.internet.defer.Deferred}
    @ivar doneDefer: the deferred that will fire when done writing
    """
    
    def __init__(self, inputStream, outFile, start = 0, length = None):
        """Initializes the file.
        
        @type inputStream: L{twisted.web2.stream.IByteStream}
        @param inputStream: the input stream to read from
        @type outFile: L{twisted.python.filepath.FilePath}
        @param outFile: the file to write to
        @type start: C{int}
        @param start: the file position to start writing at
            (optional, defaults to the start of the file)
        @type length: C{int}
        @param length: the maximum amount of data to write to the file
            (optional, defaults to not limiting the writing to the file
        """
        self.stream = inputStream
        self.outFile = outFile
        self.hash = sha.new()
        self.position = start
        self.length = None
        if length is not None:
            self.length = start + length
        self.doneDefer = None
        
    def run(self):
        """Start the streaming.

        @rtype: L{twisted.internet.defer.Deferred}
        """
        log.msg('Started streaming %r bytes to file at position %d' % (self.length, self.position))
        self.doneDefer = stream.readStream(self.stream, self._gotData)
        self.doneDefer.addCallbacks(self._done, self._error)
        return self.doneDefer

    def _gotData(self, data):
        """Process the received data."""
        if self.outFile.closed:
            raise Exception, "outFile was unexpectedly closed"
        
        if data is None:
            raise Exception, "Data is None?"
        
        # Make sure we don't go too far
        if self.length is not None and self.position + len(data) > self.length:
            data = data[:(self.length - self.position)]
        
        # Write and hash the streamed data
        self.outFile.seek(self.position)
        self.outFile.write(data)
        self.hash.update(data)
        self.position += len(data)
        
    def _done(self, result):
        """Return the result."""
        log.msg('Streaming is complete')
        return self.hash.digest()
    
    def _error(self, err):
        """Log the error."""
        log.msg('Streaming error')
        log.err(err)
        return err
    
class FileDownload:
    """Manage a download from a list of peers or a mirror.
    
    @type manager: L{PeerManager}
    @ivar manager: the manager to send requests for peers to
    @type hash: L{Hash.HashObject}
    @ivar hash: the hash object containing the expected hash for the file
    @ivar mirror: the URI of the file on the mirror
    @type compact_peers: C{list} of C{dictionary}
    @ivar compact_peers: a list of the peer info where the file can be found
    @type file: C{file}
    @ivar file: the open file to right the download to
    @type path: C{string}
    @ivar path: the path to request from peers to access the file
    @type pieces: C{list} of C{string} 
    @ivar pieces: the hashes of the pieces in the file
    @type started: C{boolean}
    @ivar started: whether the download has begun yet
    @type defer: L{twisted.internet.defer.Deferred}
    @ivar defer: the deferred that will callback with the result of the download
    @type peers: C{dictionary}
    @ivar peers: information about each of the peers available to download from
    @type outstanding: C{int}
    @ivar outstanding: the number of requests to peers currently outstanding
    @type peerlist: C{list} of L{HTTPDownloader.Peer}
    @ivar peerlist: the sorted list of peers for this download
    @type stream: L{GrowingFileStream}
    @ivar stream: the stream of resulting data from the download
    @type nextFinish: C{int}
    @ivar nextFinish: the next piece that is needed to finish for the stream
    @type completePieces: C{list} of C{boolean} or L{HTTPDownloader.Peer}
    @ivar completePieces: one per piece, will be False if no requests are
        outstanding for the piece, True if the piece has been successfully
        downloaded, or the Peer that a request for this piece has been sent  
    """
    
    def __init__(self, manager, hash, mirror, compact_peers, file):
        """Initialize the instance and check for piece hashes.
        
        @type manager: L{PeerManager}
        @param manager: the manager to send requests for peers to
        @type hash: L{Hash.HashObject}
        @param hash: the hash object containing the expected hash for the file
        @param mirror: the URI of the file on the mirror
        @type compact_peers: C{list} of C{dictionary}
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
        self.file = file.open('w+')

    def run(self):
        """Start the downloading process."""
        log.msg('Checking for pieces for %s' % self.path)
        self.defer = defer.Deferred()
        self.peers = {}
        no_pieces = 0
        pieces_string = {0: 0}
        pieces_hash = {0: 0}
        pieces_dl_hash = {0: 0}

        for compact_peer in self.compact_peers:
            # Build a list of all the peers for this download
            site = uncompact(compact_peer['c'])
            peer = self.manager.getPeer(site)
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
            log.msg('No pieces were found for the file')
            self.pieces = []
            self.startDownload()
        elif max_found == max(pieces_string.values()):
            # Small number of pieces in a string
            for pieces, num in pieces_string.items():
                # Find the most popular piece string
                if num == max_found:
                    self.pieces = [pieces[x:x+20] for x in xrange(0, len(pieces), 20)]
                    log.msg('Peer info contained %d piece hashes' % len(self.pieces))
                    self.startDownload()
                    break
        elif max_found == max(pieces_hash.values()):
            # Medium number of pieces stored in the DHT
            for pieces, num in pieces_hash.items():
                # Find the most popular piece hash to lookup
                if num == max_found:
                    log.msg('Found a hash for pieces to lookup in the DHT: %r' % pieces)
                    self.getDHTPieces(pieces)
                    break
        elif max_found == max(pieces_dl_hash.values()):
            # Large number of pieces stored in peers
            for pieces, num in pieces_dl_hash.items():
                # Find the most popular piece hash to download
                if num == max_found:
                    log.msg('Found a hash for pieces to lookup in peers: %r' % pieces)
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
            log.msg('Starting the lookup of piece hashes in peers')
            self.outstanding = 0
            # Remove any peers with the wrong piece hash
            #for site in self.peers.keys():
            #    if self.peers[site].get('l', '') != key:
            #        del self.peers[site]
        else:
            log.msg('Piece hash lookup failed for peer %r' % (failedSite, ))
            self.peers[failedSite]['failed'] = True
            self.outstanding -= 1

        if self.pieces is None:
            # Send a request to one or more peers
            log.msg('Checking for a peer to request piece hashes from')
            for site in self.peers:
                if self.peers[site].get('failed', False) != True:
                    log.msg('Sending a piece hash request to %r' % (site, ))
                    path = '/~/' + quote_plus(key)
                    lookupDefer = self.peers[site]['peer'].get(path)
                    reactor.callLater(0, lookupDefer.addCallbacks,
                                      *(self._getPeerPieces, self._gotPeerError),
                                      **{'callbackArgs': (key, site),
                                         'errbackArgs': (key, site)})
                    self.outstanding += 1
                    if self.outstanding >= 3:
                        break
        
        log.msg('Done sending piece hash requests for now, %d outstanding' % self.outstanding)
        if self.pieces is None and self.outstanding <= 0:
            # Continue without the piece hashes
            log.msg('Could not retrieve the piece hashes from the peers')
            self.pieces = []
            self.startDownload()
        
    def _getPeerPieces(self, response, key, site):
        """Process the retrieved response from the peer."""
        log.msg('Got a piece hash response %d from %r' % (response.code, site))
        if response.code != 200:
            # Request failed, try a different peer
            log.msg('Did not like response %d from %r' % (response.code, site))
            self.getPeerPieces(key, site)
        else:
            # Read the response stream to a string
            self.peers[site]['pieces'] = ''
            def _gotPeerPiece(data, self = self, site = site):
                log.msg('Peer %r got %d bytes of piece hashes' % (site, len(data)))
                self.peers[site]['pieces'] += data
            log.msg('Streaming piece hashes from peer')
            df = stream.readStream(response.stream, _gotPeerPiece)
            df.addCallbacks(self._gotPeerPieces, self._gotPeerError,
                            callbackArgs=(key, site), errbackArgs=(key, site))

    def _gotPeerError(self, err, key, site):
        """Peer failed, try again."""
        log.msg('Peer piece hash request failed for %r' % (site, ))
        log.err(err)
        self.getPeerPieces(key, site)

    def _gotPeerPieces(self, result, key, site):
        """Check the retrieved pieces from the peer."""
        log.msg('Finished streaming piece hashes from peer %r' % (site, ))
        if self.pieces is not None:
            # Already done
            log.msg('Already done')
            return
        
        try:
            result = bdecode(self.peers[site]['pieces'])
        except:
            log.msg('Error bdecoding piece hashes')
            log.err()
            self.getPeerPieces(key, site)
            return
            
        result_hash = sha.new(result.get('t', '')).digest()
        if result_hash == key:
            pieces = result['t']
            self.pieces = [pieces[x:x+20] for x in xrange(0, len(pieces), 20)]
            log.msg('Retrieved %d piece hashes from the peer %r' % (len(self.pieces), site))
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
        
        log.msg('Starting to download %s' % self.path)
        self.started = True
        assert self.pieces is not None, "You must initialize the piece hashes first"
        self.peerlist = [self.peers[site]['peer'] for site in self.peers]
        
        # Special case if there's only one good peer left
        if len(self.peerlist) == 1:
            log.msg('Downloading from peer %r' % (self.peerlist[0], ))
            self.defer.callback(self.peerlist[0].get(self.path))
            return
        
        # Start sending the return file
        self.stream = GrowingFileStream(self.file, self.hash.expSize)
        resp = Response(200, {}, self.stream)
        self.defer.callback(resp)

        # Begin to download the pieces
        self.outstanding = 0
        self.nextFinish = 0
        if self.pieces:
            self.completePieces = [False for piece in self.pieces]
        else:
            self.completePieces = [False]
        self.getPieces()
        
    #{ Downloading the pieces
    def getPieces(self):
        """Download the next pieces from the peers."""
        log.msg('Checking for more piece requests to send')
        self.sort()
        piece = self.nextFinish
        while self.outstanding < 4 and self.peerlist and piece < len(self.completePieces):
            log.msg('Checking piece %d' % piece)
            if self.completePieces[piece] == False:
                # Send a request to the highest ranked peer
                peer = self.peerlist.pop()
                self.completePieces[piece] = peer
                log.msg('Sending a request for piece %d to peer %r' % (piece, peer))
                
                self.outstanding += 1
                if self.pieces:
                    df = peer.getRange(self.path, piece*PIECE_SIZE, (piece+1)*PIECE_SIZE - 1)
                else:
                    df = peer.get(self.path)
                reactor.callLater(0, df.addCallbacks,
                                  *(self._getPiece, self._getError),
                                  **{'callbackArgs': (piece, peer),
                                     'errbackArgs': (piece, peer)})
            piece += 1
                
        log.msg('Finished checking pieces, %d outstanding, next piece %d of %d' % (self.outstanding, self.nextFinish, len(self.completePieces)))
        # Check if we're done
        if self.outstanding <= 0 and self.nextFinish >= len(self.completePieces):
            log.msg('We seem to be done with all pieces')
            self.stream.allAvailable()
    
    def _getPiece(self, response, piece, peer):
        """Process the retrieved headers from the peer."""
        log.msg('Got response for piece %d from peer %r' % (piece, peer))
        if ((len(self.completePieces) > 1 and response.code != 206) or
            (response.code not in (200, 206))):
            # Request failed, try a different peer
            log.msg('Wrong response type %d for piece %d from peer %r' % (response.code, piece, peer))
            peer.hashError('Peer responded with the wrong type of download: %r' % response.code)
            self.completePieces[piece] = False
            if response.stream and response.stream.length:
                stream.readAndDiscard(response.stream)
        else:
            # Read the response stream to the file
            log.msg('Streaming piece %d from peer %r' % (piece, peer))
            if response.code == 206:
                df = StreamToFile(response.stream, self.file, piece*PIECE_SIZE, PIECE_SIZE).run()
            else:
                df = StreamToFile(response.stream, self.file).run()
            df.addCallbacks(self._gotPiece, self._gotError,
                            callbackArgs=(piece, peer), errbackArgs=(piece, peer))

        self.outstanding -= 1
        self.peerlist.append(peer)
        self.getPieces()

    def _getError(self, err, piece, peer):
        """Peer failed, try again."""
        log.msg('Got error for piece %d from peer %r' % (piece, peer))
        self.outstanding -= 1
        self.peerlist.append(peer)
        self.completePieces[piece] = False
        self.getPieces()
        log.err(err)

    def _gotPiece(self, response, piece, peer):
        """Process the retrieved piece from the peer."""
        log.msg('Finished streaming piece %d from peer %r: %r' % (piece, peer, response))
        if ((self.pieces and response != self.pieces[piece]) or
            (len(self.pieces) == 0 and response != self.hash.expected())):
            # Hash doesn't match
            log.msg('Hash error for piece %d from peer %r' % (piece, peer))
            peer.hashError('Piece received from peer does not match expected')
            self.completePieces[piece] = False
        elif self.pieces:
            # Successfully completed one of several pieces
            log.msg('Finished with piece %d from peer %r' % (piece, peer))
            self.completePieces[piece] = True
            while (self.nextFinish < len(self.completePieces) and
                   self.completePieces[self.nextFinish] == True):
                self.nextFinish += 1
                self.stream.updateAvailable(PIECE_SIZE)
        else:
            # Whole download (only one piece) is complete
            log.msg('Piece %d from peer %r is the last piece' % (piece, peer))
            self.completePieces[piece] = True
            self.nextFinish = 1
            self.stream.updateAvailable(2**30)

        self.getPieces()

    def _gotError(self, err, piece, peer):
        """Piece download failed, try again."""
        log.msg('Error streaming piece %d from peer %r: %r' % (piece, peer, response))
        log.err(err)
        self.completePieces[piece] = False
        self.getPieces()
        
class PeerManager:
    """Manage a set of peers and the requests to them.
    
    @type cache_dir: L{twisted.python.filepath.FilePath}
    @ivar cache_dir: the directory to use for storing all files
    @type dht: L{interfaces.IDHT}
    @ivar dht: the DHT instance
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
