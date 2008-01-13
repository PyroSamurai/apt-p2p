
from datetime import datetime
import os, sha, random

from twisted.internet import defer, reactor
from twisted.internet.abstract import isIPAddress
from twisted.python import log
from twisted.trial import unittest
from zope.interface import implements

from apt_dht.interfaces import IDHT
from khashmir import Khashmir

class DHTError(Exception):
    """Represents errors that occur in the DHT."""

class DHT:
    
    implements(IDHT)
    
    def __init__(self):
        self.config = None
        self.cache_dir = ''
        self.bootstrap = []
        self.bootstrap_node = False
        self.joining = None
        self.joined = False
        self.outstandingJoins = 0
        self.foundAddrs = []
        self.storing = {}
        self.retrieving = {}
        self.retrieved = {}
    
    def loadConfig(self, config, section):
        """See L{apt_dht.interfaces.IDHT}."""
        self.config_parser = config
        self.section = section
        self.config = {}
        self.cache_dir = self.config_parser.get(section, 'cache_dir')
        self.bootstrap = self.config_parser.getstringlist(section, 'BOOTSTRAP')
        self.bootstrap_node = self.config_parser.getboolean(section, 'BOOTSTRAP_NODE')
        for k in self.config_parser.options(section):
            if k in ['K', 'HASH_LENGTH', 'CONCURRENT_REQS', 'STORE_REDUNDANCY', 
                     'MAX_FAILURES', 'PORT']:
                self.config[k] = self.config_parser.getint(section, k)
            elif k in ['CHECKPOINT_INTERVAL', 'MIN_PING_INTERVAL', 
                       'BUCKET_STALENESS', 'KEINITIAL_DELAY', 'KE_DELAY', 'KE_AGE']:
                self.config[k] = self.config_parser.gettime(section, k)
            elif k in ['SPEW']:
                self.config[k] = self.config_parser.getboolean(section, k)
            else:
                self.config[k] = self.config_parser.get(section, k)
    
    def join(self):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"
        if self.joining:
            raise DHTError, "a join is already in progress"

        self.khashmir = Khashmir(self.config, self.cache_dir)
        
        self.joining = defer.Deferred()
        for node in self.bootstrap:
            host, port = node.rsplit(':', 1)
            port = int(port)
            if isIPAddress(host):
                self._join_gotIP(host, port)
            else:
                reactor.resolve(host).addCallback(self._join_gotIP, port)
        
        return self.joining

    def _join_gotIP(self, ip, port):
        """Called after an IP address has been found for a single bootstrap node."""
        self.outstandingJoins += 1
        self.khashmir.addContact(ip, port, self._join_single, self._join_error)
    
    def _join_single(self, addr):
        """Called when a single bootstrap node has been added."""
        self.outstandingJoins -= 1
        if addr:
            self.foundAddrs.append(addr)
        if addr or self.outstandingJoins <= 0:
            self.khashmir.findCloseNodes(self._join_complete, self._join_complete)
        log.msg('Got back from bootstrap node: %r' % (addr,))
    
    def _join_error(self, failure = None):
        """Called when a single bootstrap node has failed."""
        self.outstandingJoins -= 1
        log.msg("bootstrap node could not be reached")
        if self.outstandingJoins <= 0:
            self.khashmir.findCloseNodes(self._join_complete, self._join_complete)

    def _join_complete(self, result):
        """Called when the tables have been initialized with nodes."""
        if not self.joined and len(result) > 0:
            self.joined = True
        if self.joining and self.outstandingJoins <= 0:
            df = self.joining
            self.joining = None
            if self.joined or self.bootstrap_node:
                self.joined = True
                df.callback(self.foundAddrs)
            else:
                df.errback(DHTError('could not find any nodes to bootstrap to'))
        
    def getAddrs(self):
        return self.foundAddrs
        
    def leave(self):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"
        
        if self.joined or self.joining:
            if self.joining:
                self.joining.errback(DHTError('still joining when leave was called'))
                self.joining = None
            self.joined = False
            self.khashmir.shutdown()
        
    def getValue(self, key):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"
        if not self.joined:
            raise DHTError, "have not joined a network yet"

        d = defer.Deferred()
        if key not in self.retrieving:
            self.khashmir.valueForKey(key, self._getValue)
        self.retrieving.setdefault(key, []).append(d)
        return d
        
    def _getValue(self, key, result):
        if result:
            self.retrieved.setdefault(key, []).extend(result)
        else:
            final_result = []
            if key in self.retrieved:
                final_result = self.retrieved[key]
                del self.retrieved[key]
            for i in range(len(self.retrieving[key])):
                d = self.retrieving[key].pop(0)
                d.callback(final_result)
            del self.retrieving[key]

    def storeValue(self, key, value, originated = None):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"
        if not self.joined:
            raise DHTError, "have not joined a network yet"

        if key in self.storing and value in self.storing[key]:
            raise DHTError, "already storing that key with the same value"

        if originated is None:
            originated = datetime.utcnow()
        d = defer.Deferred()
        self.khashmir.storeValueForKey(key, value, originated, self._storeValue)
        self.storing.setdefault(key, {})[value] = d
        return d
    
    def _storeValue(self, key, value, result):
        if key in self.storing and value in self.storing[key]:
            if len(result) > 0:
                self.storing[key][value].callback(result)
            else:
                self.storing[key][value].errback(DHTError('could not store value %s in key %s' % (value, key)))
            del self.storing[key][value]
            if len(self.storing[key].keys()) == 0:
                del self.storing[key]

class TestSimpleDHT(unittest.TestCase):
    """Unit tests for the DHT."""
    
    timeout = 2
    DHT_DEFAULTS = {'PORT': 9977, 'K': 8, 'HASH_LENGTH': 160,
                    'CHECKPOINT_INTERVAL': 900, 'CONCURRENT_REQS': 4,
                    'STORE_REDUNDANCY': 3, 'MAX_FAILURES': 3,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KEINITIAL_DELAY': 15, 'KE_DELAY': 1200,
                    'KE_AGE': 3600, 'SPEW': True, }

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
        
    def node_join(self, result):
        d = self.b.join()
        return d
    
    def test_join(self):
        self.lastDefer = defer.Deferred()
        d = self.a.join()
        d.addCallback(self.node_join)
        d.addCallback(self.lastDefer.callback)
        return self.lastDefer

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
    
    timeout = 60
    num = 20
    DHT_DEFAULTS = {'PORT': 9977, 'K': 8, 'HASH_LENGTH': 160,
                    'CHECKPOINT_INTERVAL': 900, 'CONCURRENT_REQS': 4,
                    'STORE_REDUNDANCY': 3, 'MAX_FAILURES': 3,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KEINITIAL_DELAY': 15, 'KE_DELAY': 1200,
                    'KE_AGE': 3600, 'SPEW': False, }

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
