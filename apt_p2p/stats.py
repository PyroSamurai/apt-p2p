
"""Store statistics for the Apt-P2P downloader."""

from datetime import datetime, timedelta
from StringIO import StringIO

from util import uncompact, byte_format

class StatsLogger:
    """Store the statistics for the Khashmir DHT.
    
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
    
    def __init__(self, db):
        """Initialize the statistics.
        
        @type store: L{db.DB}
        @param store: the database for the Apt-P2P downloader
        """
        # Database
        self.db = db
        self.hashes, self.files = 0, 0
        
        # Transport
        self.mirrorDown = 0L
        self.peerDown = 0L
        self.peerUp = 0L
        
        # Transport All-Time
        stats = self.db.getStats()
        self.mirrorAllDown = long(stats.get('mirror_down', 0L))
        self.peerAllDown = long(stats.get('peer_down', 0L))
        self.peerAllUp = long(stats.get('peer_up', 0L))
        
    def save(self):
        """Save the persistent statistics to the DB."""
        stats = {'mirror_down': self.mirrorAllDown,
                 'peer_down': self.peerAllDown,
                 'peer_up': self.peerAllUp,
                 }
        self.db.saveStats(stats)
    
    def formatHTML(self, contactAddress):
        """Gather statistics for the DHT and format them for display in a browser.
        
        @param contactAddress: the external IP address in use
        @rtype: C{string}
        @return: the stats, formatted for display in the body of an HTML page
        """
        self.hashes, self.files = self.db.dbStats()

        out = StringIO()
        out.write('<h2>Downloader Statistics</h2>\n')
        out.write("<table border='0' cellspacing='20px'>\n<tr>\n")
        out.write('<td>\n')

        # General
        out.write("<table border='1' cellpadding='4px'>\n")
        out.write("<tr><th><h3>General</h3></th><th>Value</th></tr>\n")
        out.write("<tr title='Contact address for this peer'><td>Contact</td><td>" + str(contactAddress) + '</td></tr>\n')
        out.write("</table>\n")
        out.write('</td><td>\n')
        
        # Database
        out.write("<table border='1' cellpadding='4px'>\n")
        out.write("<tr><th><h3>Database</h3></th><th>Value</th></tr>\n")
        out.write("<tr title='Number of distinct files in the database'><td>Distinct Files</td><td>" + str(self.hashes) + '</td></tr>\n')
        out.write("<tr title='Total number of files being shared'><td>Total Files</td><td>" + str(self.files) + '</td></tr>\n')
        out.write("</table>\n")
        out.write("</td></tr><tr><td colspan='3'>\n")
        
        # Transport
        out.write("<table border='1' cellpadding='4px'>\n")
        out.write("<tr><th><h3>Transport</h3></th><th>Mirror Downloads</th><th>Peer Downloads</th><th>Peer Uploads</th></tr>\n")
        out.write("<tr><td title='Since the program was last restarted'>This Session</td>")
        out.write("<td title='Amount downloaded from mirrors'>" + byte_format(self.mirrorDown) + '</td>')
        out.write("<td title='Amount downloaded from peers'>" + byte_format(self.peerDown) + '</td>')
        out.write("<td title='Amount uploaded to peers'>" + byte_format(self.peerUp) + '</td></tr>\n')
        out.write("<tr><td title='Since the program was last restarted'>Session Ratio</td>")
        out.write("<td title='Percent of download from mirrors'>%0.2f%%</td>" %
                  (100.0 * float(self.mirrorDown) / float(max(self.mirrorDown + self.peerDown, 1)), ))
        out.write("<td title='Percent of download from peers'>%0.2f%%</td>" %
                  (100.0 * float(self.peerDown) / float(max(self.mirrorDown + self.peerDown, 1)), ))
        out.write("<td title='Percent uploaded to peers compared with all downloaded'>%0.2f%%</td></tr>\n" %
                  (100.0 * float(self.peerUp) / float(max(self.mirrorDown + self.peerDown, 1)), ))
        out.write("<tr><td title='Since the program was installed'>All-Time</td>")
        out.write("<td title='Amount downloaded from mirrors'>" + byte_format(self.mirrorAllDown) + '</td>')
        out.write("<td title='Amount downloaded from peers'>" + byte_format(self.peerAllDown) + '</td>')
        out.write("<td title='Amount uploaded to peers'>" + byte_format(self.peerAllUp) + '</td></tr>\n')
        out.write("<tr><td title='Since the program was installed'>All-Time Ratio</td>")
        out.write("<td title='Percent of download from mirrors'>%0.2f%%</td>" %
                  (100.0 * float(self.mirrorAllDown) / float(max(self.mirrorAllDown + self.peerAllDown, 1)), ))
        out.write("<td title='Percent of download from peers'>%0.2f%%</td>" %
                  (100.0 * float(self.peerAllDown) / float(max(self.mirrorAllDown + self.peerAllDown, 1)), ))
        out.write("<td title='Percent uploaded to peers compared with all downloaded'>%0.2f%%</td></tr\n>" %
                  (100.0 * float(self.peerAllUp) / float(max(self.mirrorAllDown + self.peerAllDown, 1)), ))
        out.write("</table>\n")
        out.write("</td></tr>\n")
        out.write("</table>\n")
        
        return out.getvalue()

    #{ Transport
    def sentBytes(self, bytes):
        """Record that some bytes were sent.
        
        @param bytes: the number of bytes sent
        """
        self.peerUp += bytes
        self.peerAllUp += bytes
        
    def receivedBytes(self, bytes, mirror = False):
        """Record that some bytes were received.
        
        @param bytes: the number of bytes received
        @param mirror: whether the bytes were sent to a mirror
        """
        if mirror:
            self.mirrorDown += bytes
            self.mirrorAllDown += bytes
        else:
            self.peerDown += bytes
            self.peerAllDown += bytes
