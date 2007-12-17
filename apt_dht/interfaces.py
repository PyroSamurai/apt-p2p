
"""
Some interfaces that are used by the apt-dht classes.

"""

from zope.interface import Interface

class IDHT(Interface):
    """An abstract interface for using a DHT implementation."""
    
    def loadConfig(self, config):
        """Load the DHTs configuration from a dictionary.
        
        @type config: C{dictionary}
        @param config: the dictionary of config values
        """
    
    def join(self, bootstrap_nodes):
        """Bootstrap the new DHT node into the DHT.
        
        @type bootstrap_nodes: C{list} of (C{string}, C{int})
        @param bootstrap_nodes: a list of the nodes to contact to join the DHT
        @rtype: C{Deffered}
        @return: a deferred that will fire when the node has joined
        """
        
    def leave(self):
        """Depart gracefully from the DHT.
        
        @rtype: C{Deffered}
        @return: a deferred that will fire when the node has joined
        """
        
    def getValue(self, key):
        """Get a value from the DHT for the specified key."""
        
    def storeValue(self, key, value):
        """Store a value in the DHT for the specified key."""
