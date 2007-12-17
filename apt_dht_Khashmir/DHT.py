
from zope.interface import implements

from apt_dht.interfaces import IDHT

class DHT:
    
    implements(IDHT)
    
    def __init__(self):
        pass