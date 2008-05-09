
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
from Streams import GrowingFileStream, StreamToFile
from util import uncompact
from Hash import PIECE_SIZE
from apt_p2p_Khashmir.bencode import bdecode
from apt_p2p_conf import config

class PeerError(Exception):
    """An error occurred downloading from peers."""
    
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
        self.defer = None
        self.mirror_path = None
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
            self.pieces = [self.hash.expected()]
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
        lookupDefer = self.manager.dht.get(key)
        lookupDefer.addBoth(self._getDHTPieces, key)
        
    def _getDHTPieces(self, results, key):
        """Check the retrieved values."""
        if isinstance(results, list):
            for result in results:
                # Make sure the hash matches the key
                result_hash = sha.new(result.get('t', '')).digest()
                if result_hash == key:
                    pieces = result['t']
                    self.pieces = [pieces[x:x+20] for x in xrange(0, len(pieces), 20)]
                    log.msg('Retrieved %d piece hashes from the DHT' % len(self.pieces))
                    self.startDownload()
                    return
                
            log.msg('Could not retrieve the piece hashes from the DHT')
        else:
            log.msg('Looking up piece hashes in the DHT resulted in an error: %r' % (result, ))
            
        # Continue without the piece hashes
        self.pieces = [None for x in xrange(0, self.hash.expSize, PIECE_SIZE)]
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
                    if self.outstanding >= 4:
                        break
        
        if self.pieces is None and self.outstanding <= 0:
            # Continue without the piece hashes
            log.msg('Could not retrieve the piece hashes from the peers')
            self.pieces = [None for x in xrange(0, self.hash.expSize, PIECE_SIZE)]
            self.startDownload()
        
    def _getPeerPieces(self, response, key, site):
        """Process the retrieved response from the peer."""
        log.msg('Got a piece hash response %d from %r' % (response.code, site))
        if response.code != 200:
            # Request failed, try a different peer
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
            if self.peers[a]['peer'].rank > self.peers[b]['peer'].rank:
                return 1
            elif self.peers[a]['peer'].rank < self.peers[b]['peer'].rank:
                return -1
            return 0
        self.sitelist.sort(sort)

    def startDownload(self):
        """Start the download from the peers."""
        # Don't start twice
        if self.started:
            return
        
        log.msg('Starting to download %s' % self.path)
        self.started = True
        assert self.pieces, "You must initialize the piece hashes first"
        
        self.sitelist = self.peers.keys()
        
        # Special case if there's only one good peer left
#        if len(self.sitelist) == 1:
#            log.msg('Downloading from peer %r' % (self.peers[self.sitelist[0]]['peer'], ))
#            self.defer.callback(self.peers[self.sitelist[0]]['peer'].get(self.path))
#            return
        
        # Begin to download the pieces
        self.outstanding = 0
        self.nextFinish = 0
        self.completePieces = [False for piece in self.pieces]
        self.addedMirror = False
        self.addMirror()
        self.getPieces()

    def addMirror(self):
        """Use the mirror if there are few peers."""
        if not self.addedMirror and len(self.sitelist) + self.outstanding < config.getint('DEFAULT', 'MIN_DOWNLOAD_PEERS'):
            self.addedMirror = True
            parsed = urlparse(self.mirror)
            if parsed[0] == "http":
                site = splitHostPort(parsed[0], parsed[1])
                self.mirror_path = urlunparse(('', '') + parsed[2:])
                peer = self.manager.getPeer(site, mirror = True)
                self.peers[site] = {}
                self.peers[site]['peer'] = peer
                self.sitelist.append(site)
        
    #{ Downloading the pieces
    def getPieces(self):
        """Download the next pieces from the peers."""
        if self.file.closed:
            log.msg('Download has been aborted for %s' % self.path)
            self.stream.allAvailable(remove = True)
            return
            
        self.sort()
        piece = self.nextFinish
        while self.outstanding < 4 and self.sitelist and piece < len(self.completePieces):
            if self.completePieces[piece] == False:
                # Send a request to the highest ranked peer
                site = self.sitelist.pop()
                self.completePieces[piece] = site
                log.msg('Sending a request for piece %d to peer %r' % (piece, self.peers[site]['peer']))
                
                self.outstanding += 1
                path = self.path
                if self.peers[site]['peer'].mirror:
                    path = self.mirror_path
                if len(self.completePieces) > 1:
                    df = self.peers[site]['peer'].getRange(path, piece*PIECE_SIZE, (piece+1)*PIECE_SIZE - 1)
                else:
                    df = self.peers[site]['peer'].get(path)
                reactor.callLater(0, df.addCallbacks,
                                  *(self._getPiece, self._getError),
                                  **{'callbackArgs': (piece, site),
                                     'errbackArgs': (piece, site)})
            piece += 1
                
        # Check if we're done
        if self.outstanding <= 0 and self.nextFinish >= len(self.completePieces):
            log.msg('Download is complete for %s' % self.path)
            self.stream.allAvailable(remove = True)
            
        # Check if we ran out of peers
        if self.outstanding <= 0 and not self.sitelist and False in self.completePieces:
            log.msg("Download failed, no peers left to try.")
            if self.defer:
                # Send a return error
                df = self.defer
                self.defer = None
                resp = Response(500, {}, None)
                df.callback(resp)
            else:
                # Already streaming the response, try and abort
                self.stream.allAvailable(remove = True)
    
    def _getPiece(self, response, piece, site):
        """Process the retrieved headers from the peer."""
        if response.code == 404:
            # Peer no longer has this file, move on
            log.msg('Peer sharing piece %d no longer has it: %r' % (piece, self.peers[site]['peer']))
            self.completePieces[piece] = False
            if response.stream and response.stream.length:
                stream.readAndDiscard(response.stream)
            
            # Don't add the site back, just move on
            site = None
        elif ((len(self.completePieces) > 1 and response.code != 206) or
            (response.code not in (200, 206))):
            # Request failed, try a different peer
            log.msg('Wrong response type %d for piece %d from peer %r' % (response.code, piece, self.peers[site]['peer']))
            self.peers[site]['peer'].hashError('Peer responded with the wrong type of download: %r' % response.code)
            self.completePieces[piece] = False
            self.peers[site]['errors'] = self.peers[site].get('errors', 0) + 1
            if response.stream and response.stream.length:
                stream.readAndDiscard(response.stream)

            # After 3 errors in a row, drop the peer
            if self.peers[site]['errors'] >= 3:
                site = None
        else:
            if self.defer:
                # Start sending the return file
                df = self.defer
                self.defer = None
                self.stream = GrowingFileStream(self.file, self.hash.expSize)

                # Get the headers from the peer's response
                headers = {}
                if response.headers.hasHeader('last-modified'):
                    headers['last-modified'] = response.headers.getHeader('last-modified')
                resp = Response(200, headers, self.stream)
                df.callback(resp)

            # Read the response stream to the file
            log.msg('Streaming piece %d from peer %r' % (piece, self.peers[site]['peer']))
            if response.code == 206:
                df = StreamToFile(self.hash.newPieceHasher(), response.stream,
                                  self.file, piece*PIECE_SIZE, PIECE_SIZE).run()
            else:
                df = StreamToFile(self.hash.newHasher(), response.stream,
                                  self.file).run()
            reactor.callLater(0, df.addCallbacks,
                              *(self._gotPiece, self._gotError),
                              **{'callbackArgs': (piece, site),
                                 'errbackArgs': (piece, site)})

        self.outstanding -= 1
        if site:
            self.sitelist.append(site)
        else:
            self.addMirror()
        self.getPieces()

    def _getError(self, err, piece, site):
        """Peer failed, try again."""
        log.msg('Got error for piece %d from peer %r' % (piece, self.peers[site]['peer']))
        self.outstanding -= 1
        self.peers[site]['errors'] = self.peers[site].get('errors', 0) + 1
        if self.peers[site]['errors'] < 3:
            self.sitelist.append(site)
        else:
            self.addMirror()
        self.completePieces[piece] = False
        self.getPieces()
        log.err(err)

    def _gotPiece(self, hash, piece, site):
        """Process the retrieved piece from the peer."""
        if self.pieces[piece] and hash.digest() != self.pieces[piece]:
            # Hash doesn't match
            log.msg('Hash error for piece %d from peer %r' % (piece, self.peers[site]['peer']))
            self.peers[site]['peer'].hashError('Piece received from peer does not match expected')
            self.peers[site]['errors'] = self.peers[site].get('errors', 0) + 1
            self.completePieces[piece] = False
        else:
            # Successfully completed one of several pieces
            log.msg('Finished with piece %d from peer %r' % (piece, self.peers[site]['peer']))
            self.completePieces[piece] = True
            self.peers[site]['errors'] = 0
            while (self.nextFinish < len(self.completePieces) and
                   self.completePieces[self.nextFinish] == True):
                self.nextFinish += 1
                self.stream.updateAvailable(PIECE_SIZE)

        self.getPieces()

    def _gotError(self, err, piece, site):
        """Piece download failed, try again."""
        log.msg('Error streaming piece %d from peer %r: %r' % (piece, self.peers[site]['peer'], err))
        log.err(err)
        self.peers[site]['errors'] = self.peers[site].get('errors', 0) + 1
        self.completePieces[piece] = False
        self.getPieces()
        
class PeerManager:
    """Manage a set of peers and the requests to them.
    
    @type cache_dir: L{twisted.python.filepath.FilePath}
    @ivar cache_dir: the directory to use for storing all files
    @type dht: L{DHTManager.DHT}
    @ivar dht: the DHT instance
    @type stats: L{stats.StatsLogger}
    @ivar stats: the statistics logger to record sent data to
    @type clients: C{dictionary}
    @ivar clients: the available peers that have been previously contacted
    """

    def __init__(self, cache_dir, dht, stats):
        """Initialize the instance."""
        self.cache_dir = cache_dir
        self.cache_dir.restat(False)
        if not self.cache_dir.exists():
            self.cache_dir.makedirs()
        self.dht = dht
        self.stats = stats
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
            peer = self.getPeer(site, mirror = True)
            return peer.get(path, method, modtime)
#        elif len(peers) == 1:
#            site = uncompact(peers[0]['c'])
#            log.msg('Downloading from peer %r' % (site, ))
#            path = '/~/' + quote_plus(hash.expected())
#            peer = self.getPeer(site)
#            return peer.get(path)
        else:
            tmpfile = self.cache_dir.child(hash.hexexpected())
            return FileDownload(self, hash, mirror, peers, tmpfile).run()
        
    def getPeer(self, site, mirror = False):
        """Create a new peer if necessary and return it.
        
        @type site: (C{string}, C{int})
        @param site: the IP address and port of the peer
        @param mirror: whether the peer is actually a mirror
            (optional, defaults to False)
        """
        if site not in self.clients:
            self.clients[site] = Peer(site[0], site[1], self.stats)
            if mirror:
                self.clients[site].mirror = True
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
    
    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.pending_calls = []
        if self.manager:
            self.manager.close()
            self.manager = None
