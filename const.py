from twisted.internet.default import SelectReactor ## twistedmatrix.com

reactor = SelectReactor(installSignalHandlers=0)
from twisted.internet import main
main.installReactor(reactor)

# magic id to use before we know a peer's id
NULL_ID =  20 * '\0'

# Kademlia "K" constant, this should be an even number
K = 20

# SHA1 is 160 bits long
HASH_LENGTH = 160

# checkpoint every this many seconds
CHECKPOINT_INTERVAL = 60 * 15 # fifteen minutes


### SEARCHING/STORING
# concurrent xmlrpc calls per find node/value request!
CONCURRENT_REQS = 4

# how many hosts to post to
STORE_REDUNDANCY = 3


###  ROUTING TABLE STUFF
# how many times in a row a node can fail to respond before it's booted from the routing table
MAX_FAILURES = 3

# never ping a node more often than this
MIN_PING_INTERVAL = 60 * 15 # fifteen minutes

# refresh buckets that haven't been touched in this long
BUCKET_STALENESS = 60 # one hour


###  KEY EXPIRER
# time before expirer starts running
KEINITIAL_DELAY = 15 # 15 seconds - to clean out old stuff in persistent db

# time between expirer runs
KE_DELAY = 60 * 60 # 1 hour

# expire entries older than this
KE_AGE = 60 * 60 * 24 # 24 hours
