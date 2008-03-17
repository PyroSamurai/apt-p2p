
"""Store statistics for the Khashmir DHT."""

from datetime import datetime, timedelta
from StringIO import StringIO

class StatsLogger:
    """Store the statistics for the Khashmir DHT.
    
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
        self.startTime = datetime.now().replace(microsecond=0)
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
    
    def formatHTML(self):
        """Gather statistics for the DHT and format them for display in a browser.
        
        @rtype: C{string}
        @return: the stats, formatted for display in the body of an HTML page
        """
        self.tableStats()
        self.dbStats()
        elapsed = datetime.now().replace(microsecond=0) - self.startTime
        out = StringIO()
        out.write('<h2>DHT Statistics</h2>\n')
        out.write("<table border='0' cellspacing='20px'>\n<tr>\n")
        out.write('<td>\n')
        out.write("<table border='1' cellpadding='4px'>\n")

        # General
        out.write("<tr><th><h3>General</h3></th><th>Value</th></tr>\n")
        out.write("<tr title='Elapsed time since the DHT was started'><td>Up time</td><td>" + str(elapsed) + '</td></tr>\n')
        out.write("<tr title='Whether this node is reachable by other nodes'><td>Reachable</td><td>" + str(self.reachable) + '</td></tr>\n')
        out.write("</table>\n")
        out.write('</td><td>\n')
        out.write("<table border='1' cellpadding='4px'>\n")
        
        # Routing
        out.write("<tr><th><h3>Routing Table</h3></th><th>Value</th></tr>\n")
        out.write("<tr title='The number of connected nodes'><td>Number of nodes</td><td>" + str(self.nodes) + '</td></tr>\n')
        out.write("<tr title='The estimated number of connected users in the entire DHT'><td>Total number of users</td><td>" + str(self.users) + '</td></tr>\n')
        out.write("</table>\n")
        out.write('</td><td>\n')
        out.write("<table border='1' cellpadding='4px'>\n")
        
        # Database
        out.write("<tr><th><h3>Database</h3></th><th>Value</th></tr>\n")
        out.write("<tr title='Number of distinct keys in the database'><td>Keys</td><td>" + str(self.keys) + '</td></tr>\n')
        out.write("<tr title='Total number of values stored locally'><td>Values</td><td>" + str(self.values) + '</td></tr>\n')
        out.write("</table>\n")
        out.write("</td></tr><tr><td colspan='3'>\n")
        out.write("<table border='1' cellpadding='4px'>\n")
        out.write("<tr><th><h3>Transport</h3></th><th>Packets</th><th>Bytes</th><th>Bytes/second</th></tr>\n")
        out.write("<tr title='Stats for packets received from the DHT'><td>Downloaded</td>")
        out.write('<td>' + str(self.downPackets) + '</td>')
        out.write('<td>' + str(self.downBytes) + '</td>')
        out.write('<td>%0.2f</td></tr>\n' % (self.downBytes / (elapsed.days*86400.0 + elapsed.seconds), ))
        out.write("<tr title='Stats for packets sent to the DHT'><td>Uploaded</td>")
        out.write('<td>' + str(self.upPackets) + '</td>')
        out.write('<td>' + str(self.upBytes) + '</td>')
        out.write('<td>%0.2f</td></tr>\n' % (self.upBytes / (elapsed.days*86400.0 + elapsed.seconds), ))
        out.write("</table>\n")
        out.write("</td></tr><tr><td colspan='3'>\n")
        out.write("<table border='1' cellpadding='4px'>\n")
        
        # Actions
        out.write("<tr><th><h3>Actions</h3></th><th>Started</th><th>Sent</th><th>OK</th><th>Failed</th><th>Received</th><th>Error</th></tr>\n")
        actions = self.actions.keys()
        actions.sort()
        for action in actions:
            out.write("<tr><td>" + action + "</td>")
            for i in xrange(6):
                out.write("<td>" + str(self.actions[action][i]) + "</td>")
            out.write('</tr>\n')
        out.write("</table>\n")
        out.write("</td></tr>\n")
        out.write("</table>\n")
        
        return out.getvalue()

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
