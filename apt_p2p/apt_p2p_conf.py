
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
from twisted.trial import unittest

class ConfigError(Exception):
    """Errors that occur in the loading of configuration variables."""
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return repr(self.message)

version = versions.Version('apt-p2p', 0, 1, 4)

mapbase64 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.-'
versionID = 'A'
for subver in version.base().split('.', 2):
    while type(subver) != int and len(subver) > 0:
        try:
            subver = int(subver)
        except:
            subver = subver[:-1]
    if type(subver) != int or subver >= 64:
        subver = 0
    versionID += mapbase64[subver]

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
    
    # The rate to limit sending data to peers to, in KBytes/sec.
    # Set this to 0 to not limit the upload bandwidth.
    'UPLOAD_LIMIT': '0',

    # The minimum number of peers before the mirror is not used.
    # If there are fewer peers than this for a file, the mirror will also be
    # used to speed up the download. Set to 0 to never use the mirror if
    # there are peers.
    'MIN_DOWNLOAD_PEERS': '3',

    # Directory to store the downloaded files in
    'CACHE_DIR': home + '/.apt-p2p/cache',
    
    # Other directories containing packages to share with others
    # WARNING: all files in these directories will be hashed and available
    #          for everybody to download
    'OTHER_DIRS': """""",
    
    # Whether it's OK to use an IP address from a known local/private range
    'LOCAL_OK': 'no',

    # Whether a remote peer can access the statistics page
    'REMOTE_STATS': 'yes',

    # Unload the packages cache after an interval of inactivity this long.
    # The packages cache uses a lot of memory, and only takes a few seconds
    # to reload when a new request arrives.
    'UNLOAD_PACKAGES_CACHE': '5m',

    # Refresh the DHT keys after this much time has passed.
    # This should be a time slightly less than the DHT's KEY_EXPIRE value.
    'KEY_REFRESH': '2.5h',

    # The user name to try and run as (leave blank to run as current user)
    'USERNAME': 'apt-p2p',
    
    # Which DHT implementation to use.
    # It must be possible to do "from <DHT>.DHT import DHT" to get a class that
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
    
    # checkpoint every this many seconds
    'CHECKPOINT_INTERVAL': '5m', # five minutes
    
    ### SEARCHING/STORING
    # concurrent xmlrpc calls per find node/value request!
    'CONCURRENT_REQS': '8',
    
    # how many hosts to post to
    'STORE_REDUNDANCY': '6',
    
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
    'KEY_EXPIRE': '3h', # 3 hours
    
    # Timeout KRPC requests to nodes after this time.
    'KRPC_TIMEOUT': '9s',
    
    # KRPC requests are resent using exponential backoff starting with this delay.
    # The request will first be resent after the delay set here.
    # The request will be resent again after twice the delay set here. etc.
    # e.g. if TIMEOUT is 9 sec., and INITIAL_DELAY is 2 sec., then requests will
    # be resent at times 0, 2 (2 sec. later), and 6 (4 sec. later), and then will
    # timeout at 9.
    'KRPC_INITIAL_DELAY': '2s',

    # whether to spew info about the requests/responses in the protocol
    'SPEW': 'no',
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
        return int(float(value)*mult)
    
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

class TestConfigParser(unittest.TestCase):
    """Unit tests for the config parser."""

    def test_uppercase(self):
        config.set('DEFAULT', 'case_tester', 'foo')
        self.failUnless(config.get('DEFAULT', 'CASE_TESTER') == 'foo')
        self.failUnless(config.get('DEFAULT', 'case_tester') == 'foo')
        config.set('DEFAULT', 'TEST_CASE', 'bar')
        self.failUnless(config.get('DEFAULT', 'TEST_CASE') == 'bar')
        self.failUnless(config.get('DEFAULT', 'test_case') == 'bar')
        config.set('DEFAULT', 'FINAL_test_CASE', 'foobar')
        self.failUnless(config.get('DEFAULT', 'FINAL_TEST_CASE') == 'foobar')
        self.failUnless(config.get('DEFAULT', 'final_test_case') == 'foobar')
        self.failUnless(config.get('DEFAULT', 'FINAL_test_CASE') == 'foobar')
        self.failUnless(config.get('DEFAULT', 'final_TEST_case') == 'foobar')
    
    def test_time(self):
        config.set('DEFAULT', 'time_tester_1', '2d')
        self.failUnless(config.gettime('DEFAULT', 'time_tester_1') == 2*86400)
        config.set('DEFAULT', 'time_tester_2', '3h')
        self.failUnless(config.gettime('DEFAULT', 'time_tester_2') == 3*3600)
        config.set('DEFAULT', 'time_tester_3', '4m')
        self.failUnless(config.gettime('DEFAULT', 'time_tester_3') == 4*60)
        config.set('DEFAULT', 'time_tester_4', '37s')
        self.failUnless(config.gettime('DEFAULT', 'time_tester_4') == 37)
        
    def test_floating_time(self):
        config.set('DEFAULT', 'time_float_tester_1', '2.5d')
        self.failUnless(config.gettime('DEFAULT', 'time_float_tester_1') == int(2.5*86400))
        config.set('DEFAULT', 'time_float_tester_2', '0.5h')
        self.failUnless(config.gettime('DEFAULT', 'time_float_tester_2') == int(0.5*3600))
        config.set('DEFAULT', 'time_float_tester_3', '4.3333m')
        self.failUnless(config.gettime('DEFAULT', 'time_float_tester_3') == int(4.3333*60))
        
    def test_string(self):
        config.set('DEFAULT', 'string_test', 'foobar')
        self.failUnless(type(config.getstring('DEFAULT', 'string_test')) == str)
        self.failUnless(config.getstring('DEFAULT', 'string_test') == 'foobar')

    def test_stringlist(self):
        config.set('DEFAULT', 'stringlist_test', """foo
        bar   
        foobar  """)
        l = config.getstringlist('DEFAULT', 'stringlist_test')
        self.failUnless(type(l) == list)
        self.failUnless(len(l) == 3)
        self.failUnless(l[0] == 'foo')
        self.failUnless(l[1] == 'bar')
        self.failUnless(l[2] == 'foobar')
        