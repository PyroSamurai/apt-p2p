## Copyright 2002 Andrew Loewenstern, All Rights Reserved

import hash
from bisect import *
import time
from types import *

from node import Node

# The all-powerful, magical Kademlia "k" constant, bucket depth
K = 8

# how many bits wide is our hash?
HASH_LENGTH = 160


# the local routing table for a kademlia like distributed hash table
class KTable:
    def __init__(self, node):
	# this is the root node, a.k.a. US!
	self.node = node
	self.buckets = [KBucket([], 0L, 2L**HASH_LENGTH)]
	self.insertNode(node)
	
    def _bucketIndexForInt(self, int):
	"""returns the index of the bucket that should hold int"""
	return bisect_left(self.buckets, int)
    
    def findNodes(self, id):
	""" return k nodes in our own local table closest to the ID
	    ignoreSelf means we will return K closest nodes to ourself if we search for our own ID
	    note, K closest nodes may actually include ourself, it's the callers responsibilty to 
	    not send messages to itself if it matters
	"""
	if type(id) == StringType:
	    int = hash.intify(id)
	elif type(id) == InstanceType:
	    int = id.int
	elif type(id) == IntType or type(id) == LongType:
	    int = id
	else:
	    raise TypeError, "findLocalNodes requires an int, string, or Node instance type"
	    
	nodes = []
	
	def sort(a, b, int=int):
	    """ this function is for sorting nodes relative to the ID we are looking for """
	    x, y = int ^ a.int, int ^ b.int
	    if x > y:
		return 1
	    elif x < y:
		return -1
	    return 0
	    
	i = self._bucketIndexForInt(int)
	
	## see if this node is already in our table and return it
	try:
	    index = self.buckets[i].l.index(int)
	except ValueError:
	    pass
	else:
	    self.buckets[i].touch()
	    return [self.buckets[i].l[index]]
	    
	nodes = nodes + self.buckets[i].l
	if len(nodes) == K:
	    nodes.sort(sort)
	    return nodes
	else:
	    # need more nodes
	    min = i - 1
	    max = i + 1
	    while (len(nodes) < K and (min >= 0 and max < len(self.buckets))):
		if min >= 0:
		    nodes = nodes + self.buckets[min].l
		    self.buckets[min].touch()
		if max < len(self.buckets):
		    nodes = nodes + self.buckets[max].l
		    self.buckets[max].touch()
		
	    nodes.sort(sort)
	    return nodes[:K-1]

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
	""" this is used by clients to replace a node returned by insertNode after
	    it fails to respond to a Pong message
	"""
	i = self._bucketIndexForInt(stale.int)
	try:
	    it = self.buckets[i].l.index(stale.int)
	except ValueError:
	    return

	del(self.buckets[i].l[it])
	self.buckets[i].l.append(new)

    def insertNode(self, node):
	""" 
	this insert the node, returning None if successful, returns the oldest node in the bucket if it's full
	the caller responsible for pinging the returned node and calling replaceStaleNode if it is found to be stale!!
	"""
	assert(node.id != " "*20)
	# get the bucket for this node
	i = self. _bucketIndexForInt(node.int)
	## check to see if node is in the bucket already
	try:
	    it = self.buckets[i].l.index(node.int)
	except ValueError:
	    ## no
	    pass
	else:
	    node.updateLastSeen()
	    # move node to end of bucket
	    del(self.buckets[i].l[it])
	    self.buckets[i].l.append(node)
	    self.buckets[i].touch()
	    return
	
	# we don't have this node, check to see if the bucket is full
	if len(self.buckets[i].l) < K:
	    # no, append this node and return
	    self.buckets[i].l.append(node)
	    self.buckets[i].touch()
	    return
	    
	# bucket is full, check to see if self.node is in the bucket
	try:
	    me = self.buckets[i].l.index(self.node) 
	except ValueError:
	    return self.buckets[i].l[0]
	
	## this bucket is full and contains our node, split the bucket
	if len(self.buckets) >= HASH_LENGTH:
	    # our table is FULL
	    print "Hash Table is FULL!  Increase K!"
	    return
	    
	self._splitBucket(self.buckets[i])
	
	## now that the bucket is split and balanced, try to insert the node again
	return self.insertNode(node)

    def justSeenNode(self, node):
	""" call this any time you get a message from a node, to update it in the table if it's there """
	try:
	    n = self.findNodes(node.int)[0]
	except IndexError:
	    return None
	else:
	    tstamp = n.lastSeen
	    n.updateLastSeen()
	    return tstamp


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
	try:
	    return self.l[self.l.index(int)]
	    self.touch()
	except IndexError:
	    raise ValueError
	
    def __repr__(self):
	return "<KBucket items: %d     min: %d\tmax: %d>" % (len(self.l), self.min, self.max)
	
    ## comparators, necessary for bisecting list of buckets with a hash expressed as an integer or a distance
    ## compares integer or node object with the bucket's range
    def __lt__(self, a):
	if type(a) == InstanceType:
	    a = a.int
	return self.max <= a
    def __le__(self, a):
    	if type(a) == InstanceType:
	    a = a.int
	return self.min < a
    def __gt__(self, a):
    	if type(a) == InstanceType:
	    a = a.int
	return self.min > a
    def __ge__(self, a):
    	if type(a) == InstanceType:
	    a = a.int
	return self.max >= a
    def __eq__(self, a):
    	if type(a) == InstanceType:
	    a = a.int
	return self.min <= a and self.max > a
    def __ne__(self, a):
    	if type(a) == InstanceType:
	    a = a.int
	return self.min >= a or self.max < a



##############
import unittest

class TestKTable(unittest.TestCase):
    def setUp(self):
	self.a = Node(hash.newID(), 'localhost', 2002)
	self.t = KTable(self.a)

    def test_replace_stale_node(self):
	self.b = Node(hash.newID(), 'localhost', 2003)
	self.t.replaceStaleNode(self.a, self.b)
	assert(len(self.t.buckets[0].l) == 1)
	assert(self.t.buckets[0].l[0].id == self.b.id)

if __name__ == "__main__":
    unittest.main()