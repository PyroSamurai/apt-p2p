from twisted.internet.default import SelectReactor ## twistedmatrix.com

reactor = SelectReactor(installSignalHandlers=0)
from twisted.internet import main
main.installReactor(reactor)


# concurrent xmlrpc calls per find node/value request!
CONCURRENT_REQS = 3

# how many times in a row a node can fail to respond before it's booted from the routing table
MAX_FAILURES = 3

# time before expirer starts running
KEINITIAL_DELAY = 60 * 60 * 24 # 24 hours

# time between expirer runs
KE_DELAY = 60 * 60 # 1 hour

# expire entries older than this
KE_AGE = KEINITIAL_DELAY

# never ping a node more often than this
MIN_PING_INTERVAL = 60 * 15 # fifteen minutes
