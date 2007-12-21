
from twisted.internet import defer
from zope.interface import implements

from apt_dht.interfaces import IDHT
from khashmir import Khashmir

class DHTError(Exception):
    """Represents errors that occur in the DHT."""

class DHT:
    
    implements(IDHT)
    
    def __init__(self):
        self.config = None
    
    def loadConfig(self, config, section):
        """See L{apt_dht.interfaces.IDHT}."""
        self.config_parser = config
        self.section = section
        self.config = []
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

        self.khashmir = Khashmir(self.config, self.config_parser.get('DEFAULT', 'cache_dir'))
        
        for node in self.config_parser.get(self.section, 'BOOTSTRAP'):
            host, port = node.rsplit(':', 1)
            self.khashmir.addContact(host, port)
            
        self.khashmir.findCloseNodes()
        
    def leave(self):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"

        self.khashmir.listenport.stopListening()
        
    def getValue(self, key):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"

        d = defer.Deferred()
        self.khashmir.valueForKey(key, d.callback)
        return d
        
    def storeValue(self, key, value):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"

        self.khashmir.storeValueForKey(key, value)
