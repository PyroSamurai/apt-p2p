## Copyright 2002 Andrew Loewenstern, All Rights Reserved

import time
from bisect import *
from types import *

import hash
import const
from const import K, HASH_LENGTH
from node import Node

class KTable:
    """local routing table for a kademlia like distributed hash table"""
    def __init__(self, node):
        # this is the root node, a.k.a. US!
        self.node = node
        self.buckets = [KBucket([], 0L, 2L**HASH_LENGTH)]
        self.insertNode(node)
        
    def _bucketIndexForInt(self, int):
        """the index of the bucket that should hold int"""
        return bisect_left(self.buckets, int)
    
    def findNodes(self, id):
        """k nodes in our own local table closest to the ID.
        
        NOTE: response may actually include ourself, it's your responsibilty 
        to not send messages to yourself if it matters."""
        
        if isinstance(id, str):
            int = hash.intify(id)
        elif isinstance(id, Node):
            int = id.int
        elif isinstance(id, int) or isinstance(id, long):
            int = id
        else:
            raise TypeError, "findNodes requires an int, string, or Node"
            
        nodes = []
        i = self._bucketIndexForInt(int)
        
        # if this node is already in our table then return it
        try:
            index = self.buckets[i].l.index(int)
        except ValueError:
            pass
        else:
            return [self.buckets[i].l[index]]            
        nodes = nodes + self.buckets[i].l
        if len(nodes) < K:
            # need more nodes
            min = i - 1
            max = i + 1
            while len(nodes) < K and (min >= 0 or max < len(self.buckets)):
                #ASw: note that this requires K be even
                if min >= 0:
                    nodes = nodes + self.buckets[min].l
                if max < len(self.buckets):
                    nodes = nodes + self.buckets[max].l
                min = min - 1
                max = max + 1

        nodes.sort(lambda a, b, int=int: cmp(int ^ a.int, int ^ b.int))
        return nodes[:K]
        
    def _splitBucket(self, a):
        diff = (a.max - a.min) / 2
        b = KBucket([], a.max - diff, a.max)
        self.buckets.insert(self.buckets.index(a.min) + 1, b)
        a.max = a.max - diff
        # transfer nodes to new bucket
        for anode in a.l[:]:
            if anode.int >= a.max:
                a.l.remove(anode)
                b.l.append(anode)

    def replaceStaleNode(self, stale, new):
        """this is used by clients to replace a node returned by insertNode after
        it fails to respond to a Pong message"""
        i = self._bucketIndexForInt(stale.int)
        try:
            it = self.buckets[i].l.index(stale.int)
        except ValueError:
            return

        del(self.buckets[i].l[it])
        if new:
            self.buckets[i].l.append(new)

    def insertNode(self, node, contacted=1):
        """ 
        this insert the node, returning None if successful, returns the oldest node in the bucket if it's full
        the caller responsible for pinging the returned node and calling replaceStaleNode if it is found to be stale!!
        contacted means that yes, we contacted THEM and we know the node is available
        """
        assert node.id != " "*20
        if node.id == self.node.id: return
        # get the bucket for this node
        i = self. _bucketIndexForInt(node.int)
        # check to see if node is in the bucket already
        try:
            it = self.buckets[i].l.index(node.int)
        except ValueError:
            # no
            pass
        else:
            if contacted:
                node.updateLastSeen()
                # move node to end of bucket
                xnode = self.buckets[i].l[it]
                del(self.buckets[i].l[it])
                # note that we removed the original and replaced it with the new one
                # utilizing this nodes new contact info
                self.buckets[i].l.append(xnode)
                self.buckets[i].touch()
            return
        
        # we don't have this node, check to see if the bucket is full
        if len(self.buckets[i].l) < K:
            # no, append this node and return
            if contacted:
                node.updateLastSeen()
            self.buckets[i].l.append(node)
            self.buckets[i].touch()
            return
            
        # bucket is full, check to see if self.node is in the bucket
        if not (self.buckets[i].min <= self.node < self.buckets[i].max):
            return self.buckets[i].l[0]
        
        # this bucket is full and contains our node, split the bucket
        if len(self.buckets) >= HASH_LENGTH:
            # our table is FULL, this is really unlikely
            print "Hash Table is FULL!  Increase K!"
            return
            
        self._splitBucket(self.buckets[i])
        
        # now that the bucket is split and balanced, try to insert the node again
        return self.insertNode(node)

    def justSeenNode(self, node):
        """call this any time you get a message from a node
        it will update it in the table if it's there """
        try:
            n = self.findNodes(node.int)[0]
        except IndexError:
            return None
        else:
            tstamp = n.lastSeen
            n.updateLastSeen()
            return tstamp

    def nodeFailed(self, node):
        """ call this when a node fails to respond to a message, to invalidate that node """
        try:
            n = self.findNodes(node.int)[0]
        except IndexError:
            return None
        else:
            if n.msgFailed() >= const.MAX_FAILURES:
                self.replaceStaleNode(n, None)
        
class KBucket:
    __slots = ['min', 'max', 'lastAccessed']
    def __init__(self, contents, min, max):
        self.l = contents
        self.min = min
        self.max = max
        self.lastAccessed = time.time()
        
    def touch(self):
        self.lastAccessed = time.time()

    def getNodeWithInt(self, int):
        if int in self.l: return int
        else: raise ValueError
        
    def __repr__(self):
        return "<KBucket %d items (%d to %d)>" % (len(self.l), self.min, self.max)
    
    ## Comparators    
    # necessary for bisecting list of buckets with a hash expressed as an integer or a distance
    # compares integer or node object with the bucket's range
    def __lt__(self, a):
        if isinstance(a, Node): a = a.int
        return self.max <= a
    def __le__(self, a):
        if isinstance(a, Node): a = a.int
        return self.min < a
    def __gt__(self, a):
        if isinstance(a, Node): a = a.int
        return self.min > a
    def __ge__(self, a):
        if isinstance(a, Node): a = a.int
        return self.max >= a
    def __eq__(self, a):
        if isinstance(a, Node): a = a.int
        return self.min <= a and self.max > a
    def __ne__(self, a):
        if isinstance(a, Node): a = a.int
        return self.min >= a or self.max < a


### UNIT TESTS ###
import unittest

class TestKTable(unittest.TestCase):
    def setUp(self):
        self.a = Node().init(hash.newID(), 'localhost', 2002)
        self.t = KTable(self.a)
        print self.t.buckets[0].l

    def test_replace_stale_node(self):
        self.b = Node().init(hash.newID(), 'localhost', 2003)
        self.t.replaceStaleNode(self.a, self.b)
        assert len(self.t.buckets[0].l) == 1
        assert self.t.buckets[0].l[0].id == self.b.id

if __name__ == "__main__":
    unittest.main()
