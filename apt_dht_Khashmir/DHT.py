
import os

from twisted.internet import defer
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
    
    def loadConfig(self, config, section):
        """See L{apt_dht.interfaces.IDHT}."""
        self.config_parser = config
        self.section = section
        self.config = []
        self.cache_dir = self.config_parser.get('DEFAULT', 'cache_dir')
        self.bootstrap = self.config_parser.getstringlist(section, 'BOOTSTRAP')
        self.bootstrap_node = self.config_parser.getboolean(section, 'BOOTSTRAP_NODE')
        for k in self.config_parser.options(section):
            if k in ['K', 'HASH_LENGTH', 'CONCURRENT_REQS', 'STORE_REDUNDANCY', 
                     'MAX_FAILURES', 'PORT']:
                self.config[k] = self.config_parser.getint(section, k)
            elif k in ['CHECKPOINT_INTERVAL', 'MIN_PING_INTERVAL', 
                       'BUCKET_STALENESS', 'KEINITIAL_DELAY', 'KE_DELAY', 'KE_AGE']:
                self.config[k] = self.config_parser.gettime(section, k)
            else:
                self.config[k] = self.config_parser.get(section, k)
        if 'PORT' not in self.config:
            self.config['PORT'] = self.config_parser.getint('DEFAULT', 'PORT')
    
    def join(self):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"

        self.khashmir = Khashmir(self.config, self.cache_dir)
        
        self.joining = defer.Deferred()
        for node in self.bootstrap:
            host, port = node.rsplit(':', 1)
            port = int(port)
            self.khashmir.addContact(host, port, self._join_single)
        
        return self.joining
    
    def _join_single(self):
        """Called when a single bootstrap node has been added."""
        self.khashmir.findCloseNodes(self._join_complete)
    
    def _join_complete(self, result):
        """Called when the tables have been initialized with nodes."""
        if not self.joined:
            self.joined = True
            if len(result) > 0 or self.bootstrap_node:
                self.joining.callback(result)
            else:
                self.joining.errback(DHTError('could not find any nodes to bootstrap to'))
        
    def leave(self):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"
        
        if self.joined:
            self.joined = False
            self.khashmir.shutdown()
        
    def getValue(self, key):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"
        if not self.joined:
            raise DHTError, "have not joined a network yet"

        d = defer.Deferred()
        self.khashmir.valueForKey(key, d.callback)
        return d
        
    def storeValue(self, key, value):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"
        if not self.joined:
            raise DHTError, "have not joined a network yet"

        self.khashmir.storeValueForKey(key, value)

class TestSimpleDHT(unittest.TestCase):
    """Unit tests for the DHT."""
    
    timeout = 2
    DHT_DEFAULTS = {'PORT': 9977, 'K': 8, 'HASH_LENGTH': 160,
                    'CHECKPOINT_INTERVAL': 900, 'CONCURRENT_REQS': 4,
                    'STORE_REDUNDANCY': 3, 'MAX_FAILURES': 3,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KEINITIAL_DELAY': 15, 'KE_DELAY': 1200,
                    'KE_AGE': 3600, }

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
        
    def tearDown(self):
        self.a.leave()
        try:
            os.unlink(self.a.khashmir.db)
        except:
            pass
        self.b.leave()
        try:
            os.unlink(self.b.khashmir.db)
        except:
            pass

class TestMultiDHT(unittest.TestCase):
    
    timeout = 10
    num = 20
    DHT_DEFAULTS = {'PORT': 9977, 'K': 8, 'HASH_LENGTH': 160,
                    'CHECKPOINT_INTERVAL': 900, 'CONCURRENT_REQS': 4,
                    'STORE_REDUNDANCY': 3, 'MAX_FAILURES': 3,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KEINITIAL_DELAY': 15, 'KE_DELAY': 1200,
                    'KE_AGE': 3600, }

    def setUp(self):
        self.l = []
        self.startport = 4088
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
        self.lastDefer = defer.Deferred()
        d = self.l[0].join()
        d.addCallback(self.node_join, 1)
        return self.lastDefer
        
    def tearDown(self):
        for i in self.l:
            try:
                i.leave()
                os.unlink(i.khashmir.db)
            except:
                pass
