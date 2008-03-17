
"""Store statistics for the Khashmir DHT."""

from datetime import datetime, timedelta
from copy import deepcopy

class StatsLogger:
    """Store the statistics for the Khashmir DHT.
    
    @ivar _StatsTemplate: a template for returning all the statistics
    @type config: C{dictionary}
    @ivar config: the configuration parameters for the DHT
    @ivar startTime: the time the program was started
    @ivar reachable: whether we can be contacted by other nodes
    @type table: L{ktable.KTable}
    @ivar table: the routing table for the DHT
    @ivar lastTableUpdate: the last time an update of the table stats was done
    @ivar nodes: the number of nodes connected
    @ivar users: the estimated number of total users in the DHT
    @type store: L{db.DB}
    @ivar store: the database for the DHT
    @ivar lastDBUpdate: the last time an update of the database stats was done
    @ivar keys: the number of distinct keys in the database
    @ivar values: the number of values in the database
    @ivar downPackets: the number of packets received
    @ivar upPackets: the number of packets sent
    @ivar downBytes: the number of bytes received
    @ivar upBytes: the number of bytes sent
    @ivar actions: a dictionary of the actions and their statistics, keys are
        the action name, values are a list of 5 elements for the number of
        times the action was sent, responded to, failed, received, and
        generated an error
    """
    
    _StatsTemplate = [{'name': 'uptime',
                       'group': 'General',
                       'desc': 'Up time',
                       'tip': 'The elapsed time since the program started',
                       'value': None,
                       },
                      {'name': 'reachable',
                       'group': 'General',
                       'desc': 'Reachable',
                       'tip': 'Whether other nodes can contact us (not NATted or firewalled)',
                       'value': None,
                       },
                      {'name': 'nodes',
                       'group': 'Routing Table',
                       'desc': 'Number of nodes',
                       'tip': 'The number of nodes we are connected to',
                       'value': None,
                       },
                      {'name': 'users',
                       'group': 'Routing Table',
                       'desc': 'Total number of users',
                       'tip': 'The estimated total number of users in the DHT',
                       'value': None,
                       },
                      {'name': 'keys',
                       'group': 'Database',
                       'desc': 'Keys',
                       'tip': 'The number of distinct keys in the database',
                       'value': None,
                       },
                      {'name': 'values',
                       'group': 'Database',
                       'desc': 'Values',
                       'tip': 'The total number of values in the database',
                       'value': None,
                       },
                      {'name': 'downPackets',
                       'group': 'Transport',
                       'desc': 'Downloaded packets',
                       'tip': 'The number of received packets',
                       'value': None,
                       },
                      {'name': 'upPackets',
                       'group': 'Transport',
                       'desc': 'Uploaded packets',
                       'tip': 'The number of sent packets',
                       'value': None,
                       },
                      {'name': 'downBytes',
                       'group': 'Transport',
                       'desc': 'Downloaded bytes',
                       'tip': 'The number of bytes received by the DHT',
                       'value': None,
                       },
                      {'name': 'upBytes',
                       'group': 'Transport',
                       'desc': 'Uploaded bytes',
                       'tip': 'The number of bytes sent by the DHT',
                       'value': None,
                       },
                      {'name': 'downSpeed',
                       'group': 'Transport',
                       'desc': 'Downloaded bytes/second',
                       'tip': 'The number of bytes received by the DHT per second',
                       'value': None,
                       },
                      {'name': 'upSpeed',
                       'group': 'Transport',
                       'desc': 'Uploaded bytes/second',
                       'tip': 'The number of bytes sent by the DHT per second',
                       'value': None,
                       },
                      {'name': 'actions',
                       'group': 'Actions',
                       'desc': 'Actions',
                       'tip': 'The number of requests for each action',
                       'value': None,
                       },
                      ]
    
    def __init__(self, table, store, config):
        """Initialize the statistics.
        
        @type table: L{ktable.KTable}
        @param table: the routing table for the DHT
        @type store: L{db.DB}
        @param store: the database for the DHT
        @type config: C{dictionary}
        @param config: the configuration parameters for the DHT
        """
        # General
        self.config = config
        self.startTime = datetime.now()
        self.reachable = False
        
        # Routing Table
        self.table = table
        self.lastTableUpdate = datetime.now()
        self.nodes = 0
        self.users = 0
        
        # Database
        self.store = store
        self.lastDBUpdate = datetime.now()
        self.keys = 0
        self.values = 0
        
        # Transport
        self.downPackets = 0
        self.upPackets = 0
        self.downBytes = 0L
        self.upBytes = 0L
        self.actions = {}
    
    def tableStats(self):
        """Collect some statistics about the routing table.
        
        @rtype: (C{int}, C{int})
        @return: the number of contacts in the routing table, and the estimated
            number of nodes in the entire DHT
        """
        if datetime.now() - self.lastTableUpdate > timedelta(seconds = 15):
            self.lastTableUpdate = datetime.now()
            self.nodes = reduce(lambda a, b: a + len(b.l), self.table.buckets, 0)
            self.users = self.config['K'] * (2**(len(self.table.buckets) - 1))
        return (self.nodes, self.users)
    
    def dbStats(self):
        """Collect some statistics about the database.
        
        @rtype: (C{int}, C{int})
        @return: the number of keys and values in the database
        """
        if datetime.now() - self.lastDBUpdate > timedelta(minutes = 1):
            self.lastDBUpdate = datetime.now()
            self.keys, self.values = self.store.keyStats()
        return (self.keys, self.values)
    
    def gather(self):
        """Gather all the statistics for the DHT.
        
        @rtype: C{list} of C{dictionary}
        @return: each dictionary has keys describing the statistic:
            name, group, desc, tip, and value
        """
        self.tableStats()
        self.dbStats()
        stats = self._StatsTemplate[:]
        elapsed = datetime.now() - self.startTime
        for stat in stats:
            val = getattr(self, stat['name'], None)
            if stat['name'] == 'uptime':
                stat['value'] = elapsed
            elif stat['name'] == 'actions':
                stat['value'] = deepcopy(self.actions)
            elif stat['name'] == 'downSpeed':
                stat['value'] = self.downBytes / (elapsed.days*86400.0 + elapsed.seconds + elapsed.microseconds/1000000.0)
            elif stat['name'] == 'upSpeed':
                stat['value'] = self.upBytes / (elapsed.days*86400.0 + elapsed.seconds + elapsed.microseconds/1000000.0)
            elif val is not None:
                stat['value'] = val
                
        return stats
    
    #{ Called by the action
    def startedAction(self, action):
        """Record that an action was started.
        
        @param action: the name of the action
        """
        act = self.actions.setdefault(action, [0, 0, 0, 0, 0, 0])
        act[0] += 1
    
    #{ Called by the transport
    def sentAction(self, action):
        """Record that an action was attempted.
        
        @param action: the name of the action
        """
        act = self.actions.setdefault(action, [0, 0, 0, 0, 0, 0])
        act[1] += 1
        
    def responseAction(self, response, action):
        """Record that a response to an action was received.
        
        @param response: the response
        @param action: the name of the action
        @return: the response (for use in deferreds)
        """
        act = self.actions.setdefault(action, [0, 0, 0, 0, 0, 0])
        act[2] += 1
        return response
        
    def failedAction(self, response, action):
        """Record that a failed response to an action was received.
        
        @param response: the response
        @param action: the name of the action
        @return: the response (for use in deferreds)
        """
        act = self.actions.setdefault(action, [0, 0, 0, 0, 0, 0])
        act[3] += 1
        return response
        
    def receivedAction(self, action):
        """Record that an action was received.
        
        @param action: the name of the action
        """
        self.reachable = True
        act = self.actions.setdefault(action, [0, 0, 0, 0, 0, 0])
        act[4] += 1
    
    def errorAction(self, action):
        """Record that a received action resulted in an error.
        
        @param action: the name of the action
        """
        act = self.actions.setdefault(action, [0, 0, 0, 0, 0, 0])
        act[5] += 1
    
    def sentBytes(self, bytes):
        """Record that a single packet of some bytes was sent.
        
        @param bytes: the number of bytes in the packet
        """
        self.upPackets += 1
        self.upBytes += bytes
        
    def receivedBytes(self, bytes):
        """Record that a single packet of some bytes was received.
        
        @param bytes: the number of bytes in the packet
        """
        self.downPackets += 1
        self.downBytes += bytes
