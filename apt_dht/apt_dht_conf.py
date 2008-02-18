
import os, sys
from ConfigParser import SafeConfigParser

from twisted.python import log, versions

class ConfigError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return repr(self.message)

version = versions.Version('apt-dht', 0, 0, 0)
home = os.path.expandvars('${HOME}')
if home == '${HOME}' or not os.path.isdir(home):
    home = os.path.expanduser('~')
    if not os.path.isdir(home):
        home = os.path.abspath(os.path.dirname(sys.argv[0]))
DEFAULT_CONFIG_FILES=['/etc/apt-dht/apt-dht.conf',
                      home + '/.apt-dht/apt-dht.conf']

DEFAULTS = {

    # Port to listen on for all requests (TCP and UDP)
    'PORT': '9977',
    
    # Directory to store the downloaded files in
    'CACHE_DIR': home + '/.apt-dht/cache',
    
    # Other directories containing packages to share with others
    # WARNING: all files in these directories will be hashed and available
    #          for everybody to download
    'OTHER_DIRS': """""",
    
    # User name to try and run as
    'USERNAME': '',
    
    # Whether it's OK to use an IP addres from a known local/private range
    'LOCAL_OK': 'no',

    # Which DHT implementation to use.
    # It must be possile to do "from <DHT>.DHT import DHT" to get a class that
    # implements the IDHT interface.
    'DHT': 'apt_dht_Khashmir',

    # Whether to only run the DHT (for providing only a bootstrap node)
    'DHT-ONLY': 'no',
}

DHT_DEFAULTS = {
    # bootstrap nodes to contact to join the DHT
    'BOOTSTRAP': """www.camrdale.org:9977
        steveholt.hopto.org:9976""",
    
    # whether this node is a bootstrap node
    'BOOTSTRAP_NODE': "no",
    
    # Kademlia "K" constant, this should be an even number
    'K': '8',
    
    # SHA1 is 160 bits long
    'HASH_LENGTH': '160',
    
    # checkpoint every this many seconds
    'CHECKPOINT_INTERVAL': '15m', # fifteen minutes
    
    ### SEARCHING/STORING
    # concurrent xmlrpc calls per find node/value request!
    'CONCURRENT_REQS': '4',
    
    # how many hosts to post to
    'STORE_REDUNDANCY': '3',
    
    ###  ROUTING TABLE STUFF
    # how many times in a row a node can fail to respond before it's booted from the routing table
    'MAX_FAILURES': '3',
    
    # never ping a node more often than this
    'MIN_PING_INTERVAL': '15m', # fifteen minutes
    
    # refresh buckets that haven't been touched in this long
    'BUCKET_STALENESS': '1h', # one hour
    
    ###  KEY EXPIRER
    # time before expirer starts running
    'KEINITIAL_DELAY': '15s', # 15 seconds - to clean out old stuff in persistent db
    
    # time between expirer runs
    'KE_DELAY': '20m', # 20 minutes
    
    # expire entries older than this
    'KE_AGE': '1h', # 60 minutes
    
    # whether to spew info about the requests/responses in the protocol
    'SPEW': 'yes',
}

class AptDHTConfigParser(SafeConfigParser):
    """
    Adds 'gettime' to ConfigParser to interpret the suffixes.
    """
    time_multipliers={
        's': 1,    #seconds
        'm': 60,   #minutes
        'h': 3600, #hours
        'd': 86400,#days
        }

    def gettime(self, section, option):
        mult = 1
        value = self.get(section, option)
        if len(value) == 0:
            raise ConfigError("Configuration parse error: [%s] %s" % (section, option))
        suffix = value[-1].lower()
        if suffix in self.time_multipliers.keys():
            mult = self.time_multipliers[suffix]
            value = value[:-1]
        return int(value)*mult
    def getstring(self, section, option):
        return self.get(section,option)
    def getstringlist(self, section, option):
        return self.get(section,option).split()
    def optionxform(self, option):
        return option.upper()

config = AptDHTConfigParser(DEFAULTS)
config.add_section(config.get('DEFAULT', 'DHT'))
for k in DHT_DEFAULTS:
    config.set(config.get('DEFAULT', 'DHT'), k, DHT_DEFAULTS[k])
