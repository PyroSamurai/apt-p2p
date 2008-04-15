
"""The main interface to the Khashmir DHT.

@var khashmir_dir: the name of the directory to use for DHT files
"""

from datetime import datetime
import os, sha, random

from twisted.internet import defer, reactor
from twisted.internet.abstract import isIPAddress
from twisted.python import log
from twisted.trial import unittest
from zope.interface import implements

from apt_p2p.interfaces import IDHT, IDHTStats, IDHTStatsFactory
from khashmir import Khashmir
from bencode import bencode, bdecode

try:
    from twisted.web2 import channel, server, resource, http, http_headers
    _web2 = True
except ImportError:
    _web2 = False

khashmir_dir = 'apt-p2p-Khashmir'

class DHTError(Exception):
    """Represents errors that occur in the DHT."""

class DHT:
    """The main interface instance to the Khashmir DHT.
    
    @type config: C{dictionary}
    @ivar config: the DHT configuration values
    @type cache_dir: C{string}
    @ivar cache_dir: the directory to use for storing files
    @type bootstrap: C{list} of C{string}
    @ivar bootstrap: the nodes to contact to bootstrap into the system
    @type bootstrap_node: C{boolean}
    @ivar bootstrap_node: whether this node is a bootstrap node
    @type joining: L{twisted.internet.defer.Deferred}
    @ivar joining: if a join is underway, the deferred that will signal it's end
    @type joined: C{boolean}
    @ivar joined: whether the DHT network has been successfully joined
    @type outstandingJoins: C{int}
    @ivar outstandingJoins: the number of bootstrap nodes that have yet to respond
    @type next_rejoin: C{int}
    @ivar next_rejoin: the number of seconds before retrying the next join
    @type foundAddrs: C{list} of (C{string}, C{int})
    @ivar foundAddrs: the IP address an port that were returned by bootstrap nodes
    @type storing: C{dictionary}
    @ivar storing: keys are keys for which store requests are active, values
        are dictionaries with keys the values being stored and values the
        deferred to call when complete
    @type retrieving: C{dictionary}
    @ivar retrieving: keys are the keys for which getValue requests are active,
        values are lists of the deferreds waiting for the requests
    @type retrieved: C{dictionary}
    @ivar retrieved: keys are the keys for which getValue requests are active,
        values are list of the values returned so far
    @type factory: L{twisted.web2.channel.HTTPFactory}
    @ivar factory: the factory to use to serve HTTP requests for statistics
    @type config_parser: L{apt_p2p.apt_p2p_conf.AptP2PConfigParser}
    @ivar config_parser: the configuration info for the main program
    @type section: C{string}
    @ivar section: the section of the configuration info that applies to the DHT
    @type khashmir: L{khashmir.Khashmir}
    @ivar khashmir: the khashmir DHT instance to use
    """
    
    if _web2:
        implements(IDHT, IDHTStats, IDHTStatsFactory)
    else:
        implements(IDHT, IDHTStats)
        
    def __init__(self):
        """Initialize the DHT."""
        self.config = None
        self.cache_dir = ''
        self.bootstrap = []
        self.bootstrap_node = False
        self.joining = None
        self.khashmir = None
        self.joined = False
        self.outstandingJoins = 0
        self.next_rejoin = 20
        self.foundAddrs = []
        self.storing = {}
        self.retrieving = {}
        self.retrieved = {}
        self.factory = None
    
    def loadConfig(self, config, section):
        """See L{apt_p2p.interfaces.IDHT}."""
        self.config_parser = config
        self.section = section
        self.config = {}
        
        # Get some initial values
        self.cache_dir = os.path.join(self.config_parser.get(section, 'cache_dir'), khashmir_dir)
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        self.bootstrap = self.config_parser.getstringlist(section, 'BOOTSTRAP')
        self.bootstrap_node = self.config_parser.getboolean(section, 'BOOTSTRAP_NODE')
        for k in self.config_parser.options(section):
            # The numbers in the config file
            if k in ['K', 'HASH_LENGTH', 'CONCURRENT_REQS', 'STORE_REDUNDANCY', 
                     'RETRIEVE_VALUES', 'MAX_FAILURES', 'PORT']:
                self.config[k] = self.config_parser.getint(section, k)
            # The times in the config file
            elif k in ['CHECKPOINT_INTERVAL', 'MIN_PING_INTERVAL', 
                       'BUCKET_STALENESS', 'KEY_EXPIRE']:
                self.config[k] = self.config_parser.gettime(section, k)
            # The booleans in the config file
            elif k in ['SPEW']:
                self.config[k] = self.config_parser.getboolean(section, k)
            # Everything else is a string
            else:
                self.config[k] = self.config_parser.get(section, k)
    
    def join(self, deferred = None):
        """See L{apt_p2p.interfaces.IDHT}.
        
        @param deferred: the deferred to callback when the join is complete
            (optional, defaults to creating a new deferred and returning it)
        """
        # Check for multiple simultaneous joins 
        if self.joining:
            if deferred:
                deferred.errback(DHTError("a join is already in progress"))
                return
            else:
                raise DHTError, "a join is already in progress"

        if deferred:
            self.joining = deferred
        else:
            self.joining = defer.Deferred()

        if self.config is None:
            self.joining.errback(DHTError("configuration not loaded"))
            return self.joining

        # Create the new khashmir instance
        if not self.khashmir:
            self.khashmir = Khashmir(self.config, self.cache_dir)

        self.outstandingJoins = 0
        for node in self.bootstrap:
            host, port = node.rsplit(':', 1)
            port = int(port)
            self.outstandingJoins += 1
            
            # Translate host names into IP addresses
            if isIPAddress(host):
                self._join_gotIP(host, port)
            else:
                reactor.resolve(host).addCallbacks(self._join_gotIP,
                                                   self._join_resolveFailed,
                                                   callbackArgs = (port, ),
                                                   errbackArgs = (host, port))
        
        return self.joining

    def _join_gotIP(self, ip, port):
        """Join the DHT using a single bootstrap nodes IP address."""
        self.khashmir.addContact(ip, port, self._join_single, self._join_error)
    
    def _join_resolveFailed(self, err, host, port):
        """Failed to lookup the IP address of the bootstrap node."""
        log.msg('Failed to find an IP address for host: (%r, %r)' % (host, port))
        log.err(err)
        self.outstandingJoins -= 1
        if self.outstandingJoins <= 0:
            self.khashmir.findCloseNodes(self._join_complete)
    
    def _join_single(self, addr):
        """Process the response from the bootstrap node.
        
        Finish the join by contacting close nodes.
        """
        self.outstandingJoins -= 1
        if addr:
            self.foundAddrs.append(addr)
        if addr or self.outstandingJoins <= 0:
            self.khashmir.findCloseNodes(self._join_complete)
        log.msg('Got back from bootstrap node: %r' % (addr,))
    
    def _join_error(self, failure = None):
        """Process an error in contacting the bootstrap node.
        
        If no bootstrap nodes remain, finish the process by contacting
        close nodes.
        """
        self.outstandingJoins -= 1
        log.msg("bootstrap node could not be reached")
        if self.outstandingJoins <= 0:
            self.khashmir.findCloseNodes(self._join_complete)

    def _join_complete(self, result):
        """End the joining process and return the addresses found for this node."""
        if not self.joined and isinstance(result, list) and len(result) > 1:
            self.joined = True
        if self.joining and self.outstandingJoins <= 0:
            df = self.joining
            self.joining = None
            if self.joined or self.bootstrap_node:
                self.joined = True
                df.callback(self.foundAddrs)
            else:
                # Try to join later using exponential backoff delays
                log.msg('Join failed, retrying in %d seconds' % self.next_rejoin)
                reactor.callLater(self.next_rejoin, self.join, df)
                self.next_rejoin *= 2
        
    def getAddrs(self):
        """Get the list of addresses returned by bootstrap nodes for this node."""
        return self.foundAddrs
        
    def leave(self):
        """See L{apt_p2p.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"
        
        if self.joined or self.joining:
            if self.joining:
                self.joining.errback(DHTError('still joining when leave was called'))
                self.joining = None
            self.joined = False
            self.khashmir.shutdown()
        
    def _normKey(self, key, bits=None, bytes=None):
        """Normalize the length of keys used in the DHT."""
        bits = self.config["HASH_LENGTH"]
        if bits is not None:
            bytes = (bits - 1) // 8 + 1
        else:
            if bytes is None:
                raise DHTError, "you must specify one of bits or bytes for normalization"
            
        # Extend short keys with null bytes
        if len(key) < bytes:
            key = key + '\000'*(bytes - len(key))
        # Truncate long keys
        elif len(key) > bytes:
            key = key[:bytes]
        return key

    def getValue(self, key):
        """See L{apt_p2p.interfaces.IDHT}."""
        d = defer.Deferred()

        if self.config is None:
            d.errback(DHTError("configuration not loaded"))
            return d
        if not self.joined:
            d.errback(DHTError("have not joined a network yet"))
            return d
        
        key = self._normKey(key)

        if key not in self.retrieving:
            self.khashmir.valueForKey(key, self._getValue)
        self.retrieving.setdefault(key, []).append(d)
        return d
        
    def _getValue(self, key, result):
        """Process a returned list of values from the DHT."""
        # Save the list of values to return when it is complete
        if result:
            self.retrieved.setdefault(key, []).extend([bdecode(r) for r in result])
        else:
            # Empty list, the get is complete, return the result
            final_result = []
            if key in self.retrieved:
                final_result = self.retrieved[key]
                del self.retrieved[key]
            for i in range(len(self.retrieving[key])):
                d = self.retrieving[key].pop(0)
                d.callback(final_result)
            del self.retrieving[key]

    def storeValue(self, key, value):
        """See L{apt_p2p.interfaces.IDHT}."""
        d = defer.Deferred()

        if self.config is None:
            d.errback(DHTError("configuration not loaded"))
            return d
        if not self.joined:
            d.errback(DHTError("have not joined a network yet"))
            return d

        key = self._normKey(key)
        bvalue = bencode(value)

        if key in self.storing and bvalue in self.storing[key]:
            raise DHTError, "already storing that key with the same value"

        self.khashmir.storeValueForKey(key, bvalue, self._storeValue)
        self.storing.setdefault(key, {})[bvalue] = d
        return d
    
    def _storeValue(self, key, bvalue, result):
        """Process the response from the DHT."""
        if key in self.storing and bvalue in self.storing[key]:
            # Check if the store succeeded
            if len(result) > 0:
                self.storing[key][bvalue].callback(result)
            else:
                self.storing[key][bvalue].errback(DHTError('could not store value %s in key %s' % (bvalue, key)))
            del self.storing[key][bvalue]
            if len(self.storing[key].keys()) == 0:
                del self.storing[key]
    
    def getStats(self):
        """See L{apt_p2p.interfaces.IDHTStats}."""
        return self.khashmir.getStats()

    def getStatsFactory(self):
        """See L{apt_p2p.interfaces.IDHTStatsFactory}."""
        assert _web2, "NOT IMPLEMENTED: twisted.web2 must be installed to use the stats factory."
        if self.factory is None:
            # Create a simple HTTP factory for stats
            class StatsResource(resource.Resource):
                def __init__(self, manager):
                    self.manager = manager
                def render(self, ctx):
                    return http.Response(
                        200,
                        {'content-type': http_headers.MimeType('text', 'html')},
                        '<html><body>\n\n' + self.manager.getStats() + '\n</body></html>\n')
                def locateChild(self, request, segments):
                    log.msg('Got HTTP stats request from %s' % (request.remoteAddr, ))
                    return self, ()
            
            self.factory = channel.HTTPFactory(server.Site(StatsResource(self)))
        return self.factory
        

class TestSimpleDHT(unittest.TestCase):
    """Simple 2-node unit tests for the DHT."""
    
    timeout = 50
    DHT_DEFAULTS = {'PORT': 9977, 'K': 8, 'HASH_LENGTH': 160,
                    'CHECKPOINT_INTERVAL': 300, 'CONCURRENT_REQS': 4,
                    'STORE_REDUNDANCY': 3, 'RETRIEVE_VALUES': -10000,
                    'MAX_FAILURES': 3,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KEY_EXPIRE': 3600, 'SPEW': False, }

    def setUp(self):
        self.a = DHT()
        self.b = DHT()
        self.a.config = self.DHT_DEFAULTS.copy()
        self.a.config['PORT'] = 4044
        self.a.bootstrap = ["127.0.0.1:4044"]
        self.a.bootstrap_node = True
        self.a.cache_dir = '/tmp'
        self.b.config = self.DHT_DEFAULTS.copy()
        self.b.config['PORT'] = 4045
        self.b.bootstrap = ["127.0.0.1:4044"]
        self.b.cache_dir = '/tmp'
        
    def test_bootstrap_join(self):
        d = self.a.join()
        return d

    def no_krpc_errors(self, result):
        from krpc import KrpcError
        self.flushLoggedErrors(KrpcError)
        return result

    def test_failed_join(self):
        d = self.b.join()
        reactor.callLater(30, self.a.join)
        d.addCallback(self.no_krpc_errors)
        return d
        
    def node_join(self, result):
        d = self.b.join()
        return d
    
    def test_join(self):
        d = self.a.join()
        d.addCallback(self.node_join)
        return d

    def test_timeout_retransmit(self):
        d = self.b.join()
        reactor.callLater(4, self.a.join)
        return d

    def test_normKey(self):
        h = self.a._normKey('12345678901234567890')
        self.failUnless(h == '12345678901234567890')
        h = self.a._normKey('12345678901234567')
        self.failUnless(h == '12345678901234567\000\000\000')
        h = self.a._normKey('1234567890123456789012345')
        self.failUnless(h == '12345678901234567890')
        h = self.a._normKey('1234567890123456789')
        self.failUnless(h == '1234567890123456789\000')
        h = self.a._normKey('123456789012345678901')
        self.failUnless(h == '12345678901234567890')

    def value_stored(self, result, value):
        self.stored -= 1
        if self.stored == 0:
            self.get_values()
        
    def store_values(self, result):
        self.stored = 3
        d = self.a.storeValue(sha.new('4045').digest(), str(4045*3))
        d.addCallback(self.value_stored, 4045)
        d = self.a.storeValue(sha.new('4044').digest(), str(4044*2))
        d.addCallback(self.value_stored, 4044)
        d = self.b.storeValue(sha.new('4045').digest(), str(4045*2))
        d.addCallback(self.value_stored, 4045)

    def check_values(self, result, values):
        self.checked -= 1
        self.failUnless(len(result) == len(values))
        for v in result:
            self.failUnless(v in values)
        if self.checked == 0:
            self.lastDefer.callback(1)
    
    def get_values(self):
        self.checked = 4
        d = self.a.getValue(sha.new('4044').digest())
        d.addCallback(self.check_values, [str(4044*2)])
        d = self.b.getValue(sha.new('4044').digest())
        d.addCallback(self.check_values, [str(4044*2)])
        d = self.a.getValue(sha.new('4045').digest())
        d.addCallback(self.check_values, [str(4045*2), str(4045*3)])
        d = self.b.getValue(sha.new('4045').digest())
        d.addCallback(self.check_values, [str(4045*2), str(4045*3)])

    def test_store(self):
        from twisted.internet.base import DelayedCall
        DelayedCall.debug = True
        self.lastDefer = defer.Deferred()
        d = self.a.join()
        d.addCallback(self.node_join)
        d.addCallback(self.store_values)
        return self.lastDefer

    def tearDown(self):
        self.a.leave()
        try:
            os.unlink(self.a.khashmir.store.db)
        except:
            pass
        self.b.leave()
        try:
            os.unlink(self.b.khashmir.store.db)
        except:
            pass

class TestMultiDHT(unittest.TestCase):
    """More complicated 20-node tests for the DHT."""
    
    timeout = 80
    num = 20
    DHT_DEFAULTS = {'PORT': 9977, 'K': 8, 'HASH_LENGTH': 160,
                    'CHECKPOINT_INTERVAL': 300, 'CONCURRENT_REQS': 4,
                    'STORE_REDUNDANCY': 3, 'RETRIEVE_VALUES': -10000,
                    'MAX_FAILURES': 3,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KEY_EXPIRE': 3600, 'SPEW': False, }

    def setUp(self):
        self.l = []
        self.startport = 4081
        for i in range(self.num):
            self.l.append(DHT())
            self.l[i].config = self.DHT_DEFAULTS.copy()
            self.l[i].config['PORT'] = self.startport + i
            self.l[i].bootstrap = ["127.0.0.1:" + str(self.startport)]
            self.l[i].cache_dir = '/tmp'
        self.l[0].bootstrap_node = True
        
    def node_join(self, result, next_node):
        d = self.l[next_node].join()
        if next_node + 1 < len(self.l):
            d.addCallback(self.node_join, next_node + 1)
        else:
            d.addCallback(self.lastDefer.callback)
    
    def test_join(self):
        self.timeout = 2
        self.lastDefer = defer.Deferred()
        d = self.l[0].join()
        d.addCallback(self.node_join, 1)
        return self.lastDefer
        
    def store_values(self, result, i = 0, j = 0):
        if j > i:
            j -= i+1
            i += 1
        if i == len(self.l):
            self.get_values()
        else:
            d = self.l[j].storeValue(sha.new(str(self.startport+i)).digest(), str((self.startport+i)*(j+1)))
            d.addCallback(self.store_values, i, j+1)
    
    def get_values(self, result = None, check = None, i = 0, j = 0):
        if result is not None:
            self.failUnless(len(result) == len(check))
            for v in result:
                self.failUnless(v in check)
        if j >= len(self.l):
            j -= len(self.l)
            i += 1
        if i == len(self.l):
            self.lastDefer.callback(1)
        else:
            d = self.l[i].getValue(sha.new(str(self.startport+j)).digest())
            check = []
            for k in range(self.startport+j, (self.startport+j)*(j+1)+1, self.startport+j):
                check.append(str(k))
            d.addCallback(self.get_values, check, i, j + random.randrange(1, min(len(self.l), 10)))

    def store_join(self, result, next_node):
        d = self.l[next_node].join()
        if next_node + 1 < len(self.l):
            d.addCallback(self.store_join, next_node + 1)
        else:
            d.addCallback(self.store_values)
    
    def test_store(self):
        from twisted.internet.base import DelayedCall
        DelayedCall.debug = True
        self.lastDefer = defer.Deferred()
        d = self.l[0].join()
        d.addCallback(self.store_join, 1)
        return self.lastDefer

    def tearDown(self):
        for i in self.l:
            try:
                i.leave()
                os.unlink(i.khashmir.store.db)
            except:
                pass
