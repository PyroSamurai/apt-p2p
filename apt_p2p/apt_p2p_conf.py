
"""Loading of configuration files and parameters.

@type version: L{twisted.python.versions.Version}
@var version: the version of this program
@type DEFAULT_CONFIG_FILES: C{list} of C{string}
@var DEFAULT_CONFIG_FILES: the default config files to load (in order)
@var DEFAULTS: the default config parameter values for the main program
@var DHT_DEFAULTS: the default config parameter values for the default DHT

"""

import os, sys
from ConfigParser import SafeConfigParser

from twisted.python import log, versions

class ConfigError(Exception):
    """Errors that occur in the loading of configuration variables."""
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return repr(self.message)

version = versions.Version('apt-p2p', 0, 0, 0)

# Set the home parameter
home = os.path.expandvars('${HOME}')
if home == '${HOME}' or not os.path.isdir(home):
    home = os.path.expanduser('~')
    if not os.path.isdir(home):
        home = os.path.abspath(os.path.dirname(sys.argv[0]))

DEFAULT_CONFIG_FILES=['/etc/apt-p2p/apt-p2p.conf',
                      home + '/.apt-p2p/apt-p2p.conf']

DEFAULTS = {

    # Port to listen on for all requests (TCP and UDP)
    'PORT': '9977',
    
    # Directory to store the downloaded files in
    'CACHE_DIR': home + '/.apt-p2p/cache',
    
    # Other directories containing packages to share with others
    # WARNING: all files in these directories will be hashed and available
    #          for everybody to download
    'OTHER_DIRS': """""",
    
    # User name to try and run as
    'USERNAME': '',
    
    # Whether it's OK to use an IP addres from a known local/private range
    'LOCAL_OK': 'no',

    # Unload the packages cache after an interval of inactivity this long.
    # The packages cache uses a lot of memory, and only takes a few seconds
    # to reload when a new request arrives.
    'UNLOAD_PACKAGES_CACHE': '5m',

    # Refresh the DHT keys after this much time has passed.
    # This should be a time slightly less than the DHT's KEY_EXPIRE value.
    'KEY_REFRESH': '57m',

    # Which DHT implementation to use.
    # It must be possile to do "from <DHT>.DHT import DHT" to get a class that
    # implements the IDHT interface.
    'DHT': 'apt_p2p_Khashmir',

    # Whether to only run the DHT (for providing only a bootstrap node)
    'DHT-ONLY': 'no',
}

DHT_DEFAULTS = {
    # bootstrap nodes to contact to join the DHT
    'BOOTSTRAP': """www.camrdale.org:9977""",
    
    # whether this node is a bootstrap node
    'BOOTSTRAP_NODE': "no",
    
    # Kademlia "K" constant, this should be an even number
    'K': '8',
    
    # SHA1 is 160 bits long
    'HASH_LENGTH': '160',
    
    # checkpoint every this many seconds
    'CHECKPOINT_INTERVAL': '5m', # five minutes
    
    ### SEARCHING/STORING
    # concurrent xmlrpc calls per find node/value request!
    'CONCURRENT_REQS': '4',
    
    # how many hosts to post to
    'STORE_REDUNDANCY': '3',
    
    # How many values to attempt to retrieve from the DHT.
    # Setting this to 0 will try and get all values (which could take a while if
    # a lot of nodes have values). Setting it negative will try to get that
    # number of results from only the closest STORE_REDUNDANCY nodes to the hash.
    # The default is a large negative number so all values from the closest
    # STORE_REDUNDANCY nodes will be retrieved.
    'RETRIEVE_VALUES': '-10000',

    ###  ROUTING TABLE STUFF
    # how many times in a row a node can fail to respond before it's booted from the routing table
    'MAX_FAILURES': '3',
    
    # never ping a node more often than this
    'MIN_PING_INTERVAL': '15m', # fifteen minutes
    
    # refresh buckets that haven't been touched in this long
    'BUCKET_STALENESS': '1h', # one hour
    
    # expire entries older than this
    'KEY_EXPIRE': '1h', # 60 minutes
    
    # whether to spew info about the requests/responses in the protocol
    'SPEW': 'yes',
}

class AptP2PConfigParser(SafeConfigParser):
    """Adds 'gettime' and 'getstringlist' to ConfigParser objects.
    
    @ivar time_multipliers: the 'gettime' suffixes and the multipliers needed
        to convert them to seconds
    """
    
    time_multipliers={
        's': 1,    #seconds
        'm': 60,   #minutes
        'h': 3600, #hours
        'd': 86400,#days
        }

    def gettime(self, section, option):
        """Read the config parameter as a time value."""
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
        """Read the config parameter as a string."""
        return self.get(section,option)
    
    def getstringlist(self, section, option):
        """Read the multi-line config parameter as a list of strings."""
        return self.get(section,option).split()

    def optionxform(self, option):
        """Use all uppercase in the config parameters names."""
        return option.upper()

# Initialize the default config parameters
config = AptP2PConfigParser(DEFAULTS)
config.add_section(config.get('DEFAULT', 'DHT'))
for k in DHT_DEFAULTS:
    config.set(config.get('DEFAULT', 'DHT'), k, DHT_DEFAULTS[k])