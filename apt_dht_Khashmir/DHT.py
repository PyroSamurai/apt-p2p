
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
        self.config = config
        self.section = section
        if self.config.has_option(section, 'port'):
            self.port = self.config.get(section, 'port')
        else:
            self.port = self.config.get('DEFAULT', 'port')
    
    def join(self):
        """See L{apt_dht.interfaces.IDHT}."""
        if self.config is None:
            raise DHTError, "configuration not loaded"

        self.khashmir = Khashmir('', self.port)
        
        for node in self.config.get(self.section, 'bootstrap'):
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
