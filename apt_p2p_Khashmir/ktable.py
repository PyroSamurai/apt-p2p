
"""The routing table and buckets for a kademlia-like DHT.

@var K: the Kademlia "K" constant, this should be an even number
"""

from datetime import datetime
from bisect import bisect_left
from math import log as loge

from twisted.python import log
from twisted.trial import unittest

import khash
from node import Node, NULL_ID

K = 8

class KTable:
    """Local routing table for a kademlia-like distributed hash table.
    
    @type node: L{node.Node}
    @ivar node: the local node
    @type config: C{dictionary}
    @ivar config: the configuration parameters for the DHT
    @type buckets: C{list} of L{KBucket}
    @ivar buckets: the buckets of nodes in the routing table
    """
    
    def __init__(self, node, config):
        """Initialize the first empty bucket of everything.
        
        @type node: L{node.Node}
        @param node: the local node
        @type config: C{dictionary}
        @param config: the configuration parameters for the DHT
        """
        # this is the root node, a.k.a. US!
        assert node.id != NULL_ID
        self.node = node
        self.config = config
        self.buckets = [KBucket([], 0L, 2L**(khash.HASH_LENGTH*8))]
        
    def _bucketIndexForInt(self, num):
        """Find the index of the bucket that should hold the node's ID number."""
        return bisect_left(self.buckets, num)
    
    def _nodeNum(self, id):
        """Takes different types of input and converts to the node ID number.

        @type id: C{string} or C{int} or L{node.Node}
        @param id: the ID to find nodes that are close to
        @raise TypeError: if id does not properly identify an ID
        """

        # Get the ID number from the input
        if isinstance(id, str):
            return khash.intify(id)
        elif isinstance(id, Node):
            return id.num
        elif isinstance(id, int) or isinstance(id, long):
            return id
        else:
            raise TypeError, "requires an int, string, or Node input"
            
    def findNodes(self, id):
        """Find the K nodes in our own local table closest to the ID.

        @type id: C{string} or C{int} or L{node.Node}
        @param id: the ID to find nodes that are close to
        """

        # Get the ID number from the input
        num = self._nodeNum(id)
            
        # Get the K closest nodes from the appropriate bucket
        i = self._bucketIndexForInt(num)
        nodes = self.buckets[i].list()
        
        # Make sure we have enough
        if len(nodes) < K:
            # Look in adjoining buckets for nodes
            min = i - 1
            max = i + 1
            while len(nodes) < K and (min >= 0 or max < len(self.buckets)):
                # Add the adjoining buckets' nodes to the list
                if min >= 0:
                    nodes = nodes + self.buckets[min].list()
                if max < len(self.buckets):
                    nodes = nodes + self.buckets[max].list()
                min = min - 1
                max = max + 1
    
        # Sort the found nodes by proximity to the id and return the closest K
        nodes.sort(lambda a, b, num=num: cmp(num ^ a.num, num ^ b.num))
        return nodes[:K]
        
    def touch(self, id):
        """Mark a bucket as having been looked up.

        @type id: C{string} or C{int} or L{node.Node}
        @param id: the ID in the bucket that was accessed
        """
        # Get the bucket number from the input
        num = self._nodeNum(id)
        i = self._bucketIndexForInt(num)
        
        self.buckets[i].touch()

    def _mergeBucket(self, i):
        """Merge unneeded buckets after removing a node.
        
        @type i: C{int}
        @param i: the index of the bucket that lost a node
        """
        bucketRange = self.buckets[i].max - self.buckets[i].min
        otherBucket = None

        # Find if either of the neighbor buckets is the same size
        # (this will only happen if this or the neighbour has our node ID in its range)
        if i-1 >= 0 and self.buckets[i-1].max - self.buckets[i-1].min == bucketRange:
            otherBucket = i-1
        elif i+1 < len(self.buckets) and self.buckets[i+1].max - self.buckets[i+1].min == bucketRange:
            otherBucket = i+1
            
        # Try and do a merge
        if otherBucket is not None and self.buckets[i].merge(self.buckets[otherBucket]):
            # Merge was successful, remove the old bucket
            self.buckets.pop(otherBucket)
                
            # Recurse to check if the neighbour buckets can also be merged
            self._mergeBucket(min(i, otherBucket))
    
    def replaceStaleNode(self, stale, new = None):
        """Replace a stale node in a bucket with a new one.
        
        This is used by clients to replace a node returned by insertNode after
        it fails to respond to a ping.
        
        @type stale: L{node.Node}
        @param stale: the stale node to remove from the bucket
        @type new: L{node.Node}
        @param new: the new node to add in it's place (optional, defaults to
            not adding any node in the old node's place)
        """
        # Find the stale node's bucket
        removed = False
        i = self._bucketIndexForInt(stale.num)
        try:
            self.buckets[i].remove(stale.num)
        except ValueError:
            pass
        else:
            # Removed the stale node
            removed = True
            log.msg('Removed node from routing table: %s/%s' % (stale.host, stale.port))
        
        # Insert the new node
        if new and self._bucketIndexForInt(new.num) == i and self.buckets[i].len() < K:
            self.buckets[i].add(new)
        elif removed:
            self._mergeBucket(i)
    
    def insertNode(self, node, contacted = True):
        """Try to insert a node in the routing table.
        
        This inserts the node, returning True if successful, False if the
        node could have been added if it responds to a ping, otherwise returns
        the oldest node in the bucket if it's full. The caller is then
        responsible for pinging the returned node and calling replaceStaleNode
        if it doesn't respond. contacted means that yes, we contacted THEM and
        we know the node is reachable.
        
        @type node: L{node.Node}
        @param node: the new node to try and insert
        @type contacted: C{boolean}
        @param contacted: whether the new node is known to be good, i.e.
            responded to a request (optional, defaults to True)
        @rtype: L{node.Node} or C{boolean}
        @return: True if successful (the bucket wasn't full), False if the
            node could have been added if it was contacted, otherwise
            returns the oldest node in the bucket
        """
        assert node.id != NULL_ID
        if node.id == self.node.id: return True

        # Get the bucket for this node
        i = self._bucketIndexForInt(node.num)

        # Check to see if node is in the bucket already
        try:
            self.buckets[i].node(node.num)
        except ValueError:
            pass
        else:
            # The node is already in the bucket
            if contacted:
                # It responded, so update it
                node.updateLastSeen()
                # move node to end of bucket
                self.buckets[i].remove(node.num)
                # note that we removed the original and replaced it with the new one
                # utilizing this nodes new contact info
                self.buckets[i].add(node)
            return True
        
        # We don't have this node, check to see if the bucket is full
        if self.buckets[i].len() < K:
            # Not full, append this node and return
            if contacted:
                node.updateLastSeen()
                self.buckets[i].add(node)
                log.msg('Added node to routing table: %s/%s' % (node.host, node.port))
                return True
            return False
            
        # Bucket is full, check to see if the local node is not in the bucket
        if not (self.buckets[i].min <= self.node < self.buckets[i].max):
            # Local node not in the bucket, can't split it, return the oldest node
            return self.buckets[i].oldest()
        
        # Make sure our table isn't FULL, this is really unlikely
        if len(self.buckets) >= (khash.HASH_LENGTH*8):
            log.err(RuntimeError("Hash Table is FULL! Increase K!"))
            return
            
        # This bucket is full and contains our node, split the bucket
        newBucket = self.buckets[i].split()
        self.buckets.insert(i + 1, newBucket)
        
        # Now that the bucket is split and balanced, try to insert the node again
        return self.insertNode(node)
    
    def justSeenNode(self, id):
        """Mark a node as just having been seen.
        
        Call this any time you get a message from a node, it will update it
        in the table if it's there.

        @type id: C{string} or C{int} or L{node.Node}
        @param id: the node ID to mark as just having been seen
        @rtype: C{datetime.datetime}
        @return: the old lastSeen time of the node, or None if it's not in the table
        """
        # Get the bucket number
        num = self._nodeNum(id)
        i = self._bucketIndexForInt(num)

        # Check to see if node is in the bucket
        try:
            tstamp = self.buckets[i].justSeen(num)
        except ValueError:
            return None
        else:
            return tstamp
    
    def invalidateNode(self, n):
        """Remove the node from the routing table.
        
        Forget about node n. Use this when you know that a node is invalid.
        """
        self.replaceStaleNode(n)
    
    def nodeFailed(self, node):
        """Mark a node as having failed once, and remove it if it has failed too much."""
        # Get the bucket number
        num = self._nodeNum(node)
        i = self._bucketIndexForInt(num)

        # Check to see if node is in the bucket
        try:
            n = self.buckets[i].node(num)
        except ValueError:
            return None
        else:
            # The node is in the bucket
            if n.msgFailed() >= self.config['MAX_FAILURES']:
                self.invalidateNode(n)
                        
class KBucket:
    """Single bucket of nodes in a kademlia-like routing table.
    
    @type nodes: C{list} of L{node.Node}
    @ivar nodes: the nodes that are in this bucket
    @type min: C{long}
    @ivar min: the minimum node ID that can be in this bucket
    @type max: C{long}
    @ivar max: the maximum node ID that can be in this bucket
    @type lastAccessed: C{datetime.datetime}
    @ivar lastAccessed: the last time a node in this bucket was successfully contacted
    """
    
    def __init__(self, contents, min, max):
        """Initialize the bucket with nodes.
        
        @type contents: C{list} of L{node.Node}
        @param contents: the nodes to store in the bucket
        @type min: C{long}
        @param min: the minimum node ID that can be in this bucket
        @type max: C{long}
        @param max: the maximum node ID that can be in this bucket
        """
        self.nodes = contents
        self.min = min
        self.max = max
        self.lastAccessed = datetime.now()
        
    def __repr__(self):
        return "<KBucket %d items (%f to %f, range %d)>" % (
                len(self.nodes), loge(self.min+1)/loge(2), loge(self.max)/loge(2), loge(self.max-self.min)/loge(2))
    
    #{ List-like functions
    def len(self): return len(self.nodes)
    def list(self): return list(self.nodes)
    def node(self, num): return self.nodes[self.nodes.index(num)]
    def remove(self, num): return self.nodes.pop(self.nodes.index(num))
    def oldest(self): return self.nodes[0]

    def add(self, node):
        """Add the node in the correct sorted order."""
        i = len(self.nodes)
        while i > 0 and node.lastSeen < self.nodes[i-1].lastSeen:
            i -= 1
        self.nodes.insert(i, node)
        
    def sort(self):
        """Sort the nodes in the bucket by their lastSeen time."""
        def _sort(a, b):
            """Sort nodes by their lastSeen time."""
            if a.lastSeen > b.lastSeen:
                return 1
            elif a.lastSeen < b.lastSeen:
                return -1
            return 0
        self.nodes.sort(_sort)
        
    #{ Bucket functions
    def touch(self):
        """Update the L{lastAccessed} time."""
        self.lastAccessed = datetime.now()
    
    def justSeen(self, num):
        """Mark a node as having been seen.
        
        @param num: the number of the node just seen
        """
        i = self.nodes.index(num)
        
        # The node is in the bucket
        n = self.nodes[i]
        tstamp = n.lastSeen
        n.updateLastSeen()
        
        # Move the node to the end and touch the bucket
        self.nodes.pop(i)
        self.nodes.append(n)
        
        return tstamp

    def split(self):
        """Split a bucket in two.
        
        @rtype: L{KBucket}
        @return: the new bucket split from this one
        """
        # Create a new bucket with half the (upper) range of the current bucket
        diff = (self.max - self.min) / 2
        new = KBucket([], self.max - diff, self.max)
        
        # Reduce the input bucket's (upper) range 
        self.max = self.max - diff

        # Transfer nodes to the new bucket
        for node in self.nodes[:]:
            if node.num >= self.max:
                self.nodes.remove(node)
                new.add(node)
        return new
    
    def merge(self, old):
        """Try to merge two buckets into one.
        
        @type old: L{KBucket}
        @param old: the bucket to merge into this one
        @return: whether a merge was done or not
        """
        # Decide if we should do a merge
        if len(self.nodes) + old.len() > K:
            return False

        # Set the range to cover the other's as well
        self.min = min(self.min, old.min)
        self.max = max(self.max, old.max)

        # Transfer the other's nodes to this bucket, merging the sorting
        i = 0
        for node in old.list():
            while i < len(self.nodes) and self.nodes[i].lastSeen <= node.lastSeen:
                i += 1
            self.nodes.insert(i, node)
            i += 1

        return True
                
    #{ Comparators to bisect/index a list of buckets (by their range) with either a node or a long
    def __lt__(self, a):
        if isinstance(a, Node): a = a.num
        return self.max <= a
    def __le__(self, a):
        if isinstance(a, Node): a = a.num
        return self.min < a
    def __gt__(self, a):
        if isinstance(a, Node): a = a.num
        return self.min > a
    def __ge__(self, a):
        if isinstance(a, Node): a = a.num
        return self.max >= a
    def __eq__(self, a):
        if isinstance(a, Node): a = a.num
        return self.min <= a and self.max > a
    def __ne__(self, a):
        if isinstance(a, Node): a = a.num
        return self.min >= a or self.max < a

class TestKTable(unittest.TestCase):
    """Unit tests for the routing table."""
    
    def setUp(self):
        self.a = Node(khash.newID(), '127.0.0.1', 2002)
        self.t = KTable(self.a, {'MAX_FAILURES': 3})

    def testAddNode(self):
        self.b = Node(khash.newID(), '127.0.0.1', 2003)
        self.t.insertNode(self.b)
        self.failUnlessEqual(len(self.t.buckets[0].nodes), 1)
        self.failUnlessEqual(self.t.buckets[0].nodes[0], self.b)

    def testRemove(self):
        self.testAddNode()
        self.t.invalidateNode(self.b)
        self.failUnlessEqual(len(self.t.buckets[0].nodes), 0)

    def testMergeBuckets(self):
        for i in xrange(1000):
            b = Node(khash.newID(), '127.0.0.1', 2003 + i)
            self.t.insertNode(b)
        num = len(self.t.buckets)
        i = self.t._bucketIndexForInt(self.a.num)
        for b in self.t.buckets[i].nodes[:]:
            self.t.invalidateNode(b)
        self.failUnlessEqual(len(self.t.buckets), num-1)

    def testFail(self):
        self.testAddNode()
        for i in range(self.t.config['MAX_FAILURES'] - 1):
            self.t.nodeFailed(self.b)
            self.failUnlessEqual(len(self.t.buckets[0].nodes), 1)
            self.failUnlessEqual(self.t.buckets[0].nodes[0], self.b)
            
        self.t.nodeFailed(self.b)
        self.failUnlessEqual(len(self.t.buckets[0].nodes), 0)
