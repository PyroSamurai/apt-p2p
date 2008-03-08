## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

"""The routing table and buckets for a kademlia-like DHT."""

from datetime import datetime
from bisect import bisect_left

from twisted.python import log
from twisted.trial import unittest

import khash
from node import Node, NULL_ID

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
        self.buckets = [KBucket([], 0L, 2L**self.config['HASH_LENGTH'])]
        
    def _bucketIndexForInt(self, num):
        """Find the index of the bucket that should hold the node's ID number."""
        return bisect_left(self.buckets, num)
    
    def _nodeNum(self, id):
        """Takes different types of input and converts to the node ID number.

        @type id: C{string} of C{int} or L{node.Node}
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

        @type id: C{string} of C{int} or L{node.Node}
        @param id: the ID to find nodes that are close to
        """

        # Get the ID number from the input
        num = self._nodeNum(id)
            
        # Get the K closest nodes from the appropriate bucket
        i = self._bucketIndexForInt(num)
        nodes = list(self.buckets[i].l)
        
        # Make sure we have enough
        if len(nodes) < self.config['K']:
            # Look in adjoining buckets for nodes
            min = i - 1
            max = i + 1
            while len(nodes) < self.config['K'] and (min >= 0 or max < len(self.buckets)):
                # Add the adjoining buckets' nodes to the list
                if min >= 0:
                    nodes = nodes + self.buckets[min].l
                if max < len(self.buckets):
                    nodes = nodes + self.buckets[max].l
                min = min - 1
                max = max + 1
    
        # Sort the found nodes by proximity to the id and return the closest K
        nodes.sort(lambda a, b, num=num: cmp(num ^ a.num, num ^ b.num))
        return nodes[:self.config['K']]
        
    def _splitBucket(self, a):
        """Split a bucket in two.
        
        @type a: L{KBucket}
        @param a: the bucket to split
        """
        # Create a new bucket with half the (upper) range of the current bucket
        diff = (a.max - a.min) / 2
        b = KBucket([], a.max - diff, a.max)
        self.buckets.insert(self.buckets.index(a.min) + 1, b)
        
        # Reduce the input bucket's (upper) range 
        a.max = a.max - diff

        # Transfer nodes to the new bucket
        for anode in a.l[:]:
            if anode.num >= a.max:
                a.l.remove(anode)
                b.l.append(anode)
    
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
        i = self._bucketIndexForInt(stale.num)
        try:
            it = self.buckets[i].l.index(stale.num)
        except ValueError:
            return
    
        # Remove the stale node and insert the new one
        del(self.buckets[i].l[it])
        if new:
            self.buckets[i].l.append(new)
    
    def insertNode(self, node, contacted = True):
        """Try to insert a node in the routing table.
        
        This inserts the node, returning None if successful, otherwise returns
        the oldest node in the bucket if it's full. The caller is then
        responsible for pinging the returned node and calling replaceStaleNode
        if it doesn't respond. contacted means that yes, we contacted THEM and
        we know the node is reachable.
        
        @type node: L{node.Node}
        @param node: the new node to try and insert
        @type contacted: C{boolean}
        @param contacted: whether the new node is known to be good, i.e.
            responded to a request (optional, defaults to True)
        @rtype: L{node.Node}
        @return: None if successful (the bucket wasn't full), otherwise returns the oldest node in the bucket
        """
        assert node.id != NULL_ID
        if node.id == self.node.id: return

        # Get the bucket for this node
        i = self._bucketIndexForInt(node.num)

        # Check to see if node is in the bucket already
        try:
            it = self.buckets[i].l.index(node.num)
        except ValueError:
            pass
        else:
            # The node is already in the bucket
            if contacted:
                # It responded, so update it
                node.updateLastSeen()
                # move node to end of bucket
                del(self.buckets[i].l[it])
                # note that we removed the original and replaced it with the new one
                # utilizing this nodes new contact info
                self.buckets[i].l.append(node)
                self.buckets[i].touch()
            return
        
        # We don't have this node, check to see if the bucket is full
        if len(self.buckets[i].l) < self.config['K']:
            # Not full, append this node and return
            if contacted:
                node.updateLastSeen()
            self.buckets[i].l.append(node)
            self.buckets[i].touch()
            return
            
        # Bucket is full, check to see if the local node is not in the bucket
        if not (self.buckets[i].min <= self.node < self.buckets[i].max):
            # Local node not in the bucket, can't split it, return the oldest node
            return self.buckets[i].l[0]
        
        # Make sure our table isn't FULL, this is really unlikely
        if len(self.buckets) >= self.config['HASH_LENGTH']:
            log.err("Hash Table is FULL!  Increase K!")
            return
            
        # This bucket is full and contains our node, split the bucket
        self._splitBucket(self.buckets[i])
        
        # Now that the bucket is split and balanced, try to insert the node again
        return self.insertNode(node)
    
    def justSeenNode(self, id):
        """Mark a node as just having been seen.
        
        Call this any time you get a message from a node, it will update it
        in the table if it's there.

        @type id: C{string} of C{int} or L{node.Node}
        @param id: the node ID to mark as just having been seen
        @rtype: C{datetime.datetime}
        @return: the old lastSeen time of the node, or None if it's not in the table
        """
        # Get the bucket number
        num = self._nodeNum(id)
        i = self._bucketIndexForInt(num)

        # Check to see if node is in the bucket
        try:
            it = self.buckets[i].l.index(num)
        except ValueError:
            return None
        else:
            # The node is in the bucket
            n = self.buckets[i].l[it]
            tstamp = n.lastSeen
            n.updateLastSeen()
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
            it = self.buckets[i].l.index(num)
        except ValueError:
            return None
        else:
            # The node is in the bucket
            n = self.buckets[i].l[it]
            if n.msgFailed() >= self.config['MAX_FAILURES']:
                self.invalidateNode(n)
                        
class KBucket:
    """Single bucket of nodes in a kademlia-like routing table.
    
    @type l: C{list} of L{node.Node}
    @ivar l: the nodes that are in this bucket
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
        self.l = contents
        self.min = min
        self.max = max
        self.lastAccessed = datetime.now()
        
    def touch(self):
        """Update the L{lastAccessed} time."""
        self.lastAccessed = datetime.now()
    
    def getNodeWithInt(self, num):
        """Get the node in the bucket with that number.
        
        @type num: C{long}
        @param num: the node ID to look for
        @raise ValueError: if the node ID is not in the bucket
        @rtype: L{node.Node}
        @return: the node
        """
        if num in self.l: return num
        else: raise ValueError
        
    def __repr__(self):
        return "<KBucket %d items (%d to %d)>" % (len(self.l), self.min, self.max)
    
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
        self.t = KTable(self.a, {'HASH_LENGTH': 160, 'K': 8, 'MAX_FAILURES': 3})

    def testAddNode(self):
        self.b = Node(khash.newID(), '127.0.0.1', 2003)
        self.t.insertNode(self.b)
        self.failUnlessEqual(len(self.t.buckets[0].l), 1)
        self.failUnlessEqual(self.t.buckets[0].l[0], self.b)

    def testRemove(self):
        self.testAddNode()
        self.t.invalidateNode(self.b)
        self.failUnlessEqual(len(self.t.buckets[0].l), 0)

    def testFail(self):
        self.testAddNode()
        for i in range(self.t.config['MAX_FAILURES'] - 1):
            self.t.nodeFailed(self.b)
            self.failUnlessEqual(len(self.t.buckets[0].l), 1)
            self.failUnlessEqual(self.t.buckets[0].l[0], self.b)
            
        self.t.nodeFailed(self.b)
        self.failUnlessEqual(len(self.t.buckets[0].l), 0)
