
"""Manage all delaings with the DHT.

@var DHT_PIECES: the maximum number of pieces to store with our contact info
    in the DHT
@var TORRENT_PIECES: the maximum number of pieces to store as a separate entry
    in the DHT
"""

import sha

from twisted.internet import reactor
from twisted.python import log

from interfaces import IDHTStats
from apt_p2p_conf import config
from Hash import HashObject
from util import findMyIPAddr, compact

DHT_PIECES = 4
TORRENT_PIECES = 70

class DHT:
    """Manages all the requests to a DHT.
    
    @type dhtClass: L{interfaces.IDHT}
    @ivar dhtClass: the DHT class to use
    @type db: L{db.DB}
    @ivar db: the database to use for tracking files and hashes
    @type dht: L{interfaces.IDHT}
    @ivar dht: the DHT instance
    @type my_contact: C{string}
    @ivar my_contact: the 6-byte compact peer representation of this peer's
        download information (IP address and port)
    """
    
    def __init__(self, dhtClass, db):
        """Initialize the instance.
        
        @type dhtClass: L{interfaces.IDHT}
        @param dhtClass: the DHT class to use
        """
        self.dhtClass = dhtClass
        self.db = db
        self.my_contact = None
        
    def start(self):
        self.dht = self.dhtClass()
        self.dht.loadConfig(config, config.get('DEFAULT', 'DHT'))
        df = self.dht.join()
        df.addCallbacks(self.joinComplete, self.joinError)
        return df
        
    def joinComplete(self, result):
        """Complete the DHT join process and determine our download information.
        
        Called by the DHT when the join has been completed with information
        on the external IP address and port of this peer.
        """
        my_addr = findMyIPAddr(result,
                               config.getint(config.get('DEFAULT', 'DHT'), 'PORT'),
                               config.getboolean('DEFAULT', 'LOCAL_OK'))
        if not my_addr:
            raise RuntimeError, "IP address for this machine could not be found"
        self.my_contact = compact(my_addr, config.getint('DEFAULT', 'PORT'))
        self.nextRefresh = reactor.callLater(60, self.refreshFiles)
        return (my_addr, config.getint('DEFAULT', 'PORT'))

    def joinError(self, failure):
        """Joining the DHT has failed."""
        log.msg("joining DHT failed miserably")
        log.err(failure)
        return failure
    
    def refreshFiles(self, result = None, hashes = {}):
        """Refresh any files in the DHT that are about to expire."""
        if result is not None:
            log.msg('Storage resulted in: %r' % result)

        if not hashes:
            expireAfter = config.gettime('DEFAULT', 'KEY_REFRESH')
            hashes = self.db.expiredHashes(expireAfter)
            if len(hashes.keys()) > 0:
                log.msg('Refreshing the keys of %d DHT values' % len(hashes.keys()))

        delay = 60
        if hashes:
            delay = 3
            raw_hash = hashes.keys()[0]
            self.db.refreshHash(raw_hash)
            hash = HashObject(raw_hash, pieces = hashes[raw_hash]['pieces'])
            del hashes[raw_hash]
            storeDefer = self.store(hash)
            storeDefer.addBoth(self.refreshFiles, hashes)

        if self.nextRefresh.active():
            self.nextRefresh.reset(delay)
        else:
            self.nextRefresh = reactor.callLater(delay, self.refreshFiles, None, hashes)
    
    def getStats(self):
        """Retrieve the formatted statistics for the DHT.
        
        @rtype: C{string}
        @return: the formatted HTML page containing the statistics
        """
        if IDHTStats.implementedBy(self.dhtClass):
            return self.dht.getStats()
        return "<p>DHT doesn't support statistics\n"

    def get(self, key):
        """Retrieve a hash's value from the DHT."""
        return self.dht.getValue(key)
    
    def store(self, hash):
        """Add a hash for a file to the DHT.
        
        Sets the key and value from the hash information, and tries to add
        it to the DHT.
        """
        key = hash.digest()
        value = {'c': self.my_contact}
        pieces = hash.pieceDigests()
        
        # Determine how to store any piece data
        if len(pieces) <= 1:
            pass
        elif len(pieces) <= DHT_PIECES:
            # Short enough to be stored with our peer contact info
            value['t'] = {'t': ''.join(pieces)}
        elif len(pieces) <= TORRENT_PIECES:
            # Short enough to be stored in a separate key in the DHT
            value['h'] = sha.new(''.join(pieces)).digest()
        else:
            # Too long, must be served up by our peer HTTP server
            value['l'] = sha.new(''.join(pieces)).digest()

        storeDefer = self.dht.storeValue(key, value)
        storeDefer.addCallbacks(self._store_done, self._store_error,
                                callbackArgs = (hash, ), errbackArgs = (hash.digest(), ))
        return storeDefer

    def _store_done(self, result, hash):
        """Add a key/value pair for the pieces of the file to the DHT (if necessary)."""
        log.msg('Added %s to the DHT: %r' % (hash.hexdigest(), result))
        pieces = hash.pieceDigests()
        if len(pieces) > DHT_PIECES and len(pieces) <= TORRENT_PIECES:
            # Add the piece data key and value to the DHT
            key = sha.new(''.join(pieces)).digest()
            value = {'t': ''.join(pieces)}

            storeDefer = self.dht.storeValue(key, value)
            storeDefer.addCallbacks(self._store_torrent_done, self._store_error,
                                    callbackArgs = (key, ), errbackArgs = (key, ))
            return storeDefer
        return result

    def _store_torrent_done(self, result, key):
        """Adding the pieces to the DHT is complete."""
        log.msg('Added torrent string %r to the DHT: %r' % (key, result))
        return result

    def _store_error(self, err, key):
        """Adding to the DHT failed."""
        log.msg('An error occurred adding %r to the DHT: %r' % (key, err))
        return err
    