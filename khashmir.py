## Copyright 2002 Andrew Loewenstern, All Rights Reserved

from listener import Listener
from ktable import KTable, K
from node import Node
from dispatcher import Dispatcher
from hash import newID, intify
import messages
import transactions

import time

from bsddb3 import db ## find this at http://pybsddb.sf.net/
from bsddb3._db import DBNotFoundError

# don't ping unless it's been at least this many seconds since we've heard from a peer
MAX_PING_INTERVAL = 60 * 15 # fifteen minutes

# concurrent FIND_NODE/VALUE requests!
N = 3


# this is the main class!
class Khashmir:
    __slots__ = ['listener', 'node', 'table', 'dispatcher', 'tf', 'store']
    def __init__(self, host, port):
	self.listener = Listener(host, port)
	self.node = Node(newID(), host, port)
	self.table = KTable(self.node)
	self.dispatcher = Dispatcher(self.listener, messages.BASE, self.node.id)
	self.tf = transactions.TransactionFactory(self.node.id, self.dispatcher)
	
	self.store = db.DB()
	self.store.open(None, None, db.DB_BTREE)

	#### register unsolicited incoming message handlers
	self.dispatcher.registerHandler('ping', self._pingHandler, messages.PING)
		
	self.dispatcher.registerHandler('find node', self._findNodeHandler, messages.FIND_NODE)

	self.dispatcher.registerHandler('get value', self._findValueHandler, messages.GET_VALUE)
	
	self.dispatcher.registerHandler('store value', self._storeValueHandler, messages.STORE_VALUE)
	
	
    #######
    #######  LOCAL INTERFACE    - use these methods!
    def addContact(self, host, port):
	"""
	 ping this node and add the contact info to the table on pong!
	"""
	n =Node(" "*20, host, port)  # note, we 
	self.sendPing(n)


    ## this call is async!
    def findNode(self, id, callback):
	""" returns the contact info for node, or the k closest nodes, from the global table """
	# get K nodes out of local table/cache, or the node we want
	nodes = self.table.findNodes(id)
	if len(nodes) == 1 and nodes[0].id == id :
	    # we got it in our table!
	    def tcall(t, callback=callback):
		callback(t.extras)
	    self.dispatcher.postEvent(tcall, 0, extras=nodes)
	else:
	    # create our search state
	    state = FindNode(self, self.dispatcher, id, callback)
	    # handle this in our own thread
	    self.dispatcher.postEvent(state.goWithNodes, 0, extras=nodes)
    
    
    ## also async
    def valueForKey(self, key, callback):
	""" returns the values found for key in global table """
	nodes = self.table.findNodes(key)
	# create our search state
	state = GetValue(self, self.dispatcher, key, callback)
	# handle this in our own thread
	self.dispatcher.postEvent(state.goWithNodes, 0, extras=nodes)


    ## async, but in the current implementation there is no guarantee a store does anything so there is no callback right now
    def storeValueForKey(self, key, value):
	""" stores the value for key in the global table, returns immediately, no status 
	    in this implementation, peers respond but don't indicate status to storing values
	    values are stored in peers on a first-come first-served basis
	    this will probably change so more than one value can be stored under a key
	"""
	def _storeValueForKey(nodes, tf=self.tf, key=key, value=value, response= self._storedValueHandler, default= lambda t: "didn't respond"):
	    for node in nodes:
		if node.id != self.node.id:
		    t = tf.StoreValue(node, key, value, response, default)
		    t.dispatch()
	# this call is asynch
	self.findNode(key, _storeValueForKey)
	
	
    def insertNode(self, n):
	"""
	insert a node in our local table, pinging oldest contact in bucket, if necessary
	
	If all you have is a host/port, then use addContact, which calls this function after
	receiving the PONG from the remote node.  The reason for the seperation is we can't insert
	a node into the table without it's peer-ID.  That means of course the node passed into this
	method needs to be a properly formed Node object with a valid ID.
	"""
	old = self.table.insertNode(n)
	if old and (time.time() - old.lastSeen) > MAX_PING_INTERVAL and old.id != self.node.id:
	    # the bucket is full, check to see if old node is still around and if so, replace it
	    t = self.tf.Ping(old, self._notStaleNodeHandler, self._staleNodeHandler)
	    t.newnode = n
	    t.dispatch()


    def sendPing(self, node):
	"""
	    ping a node
	"""
	t = self.tf.Ping(node, self._pongHandler, self._defaultPong)
	t.dispatch()


    def findCloseNodes(self):
	"""
	    This does a findNode on the ID one away from our own.  
	    This will allow us to populate our table with nodes on our network closest to our own.
	    This is called as soon as we start up with an empty table
	"""
	id = self.node.id[:-1] + chr((ord(self.node.id[-1]) + 1) % 256)
	def callback(nodes):
	    pass
	self.findNode(id, callback)

    def refreshTable(self):
	"""
	    
	"""
	def callback(nodes):
	    pass

	for bucket in self.table.buckets:
	    if time.time() - bucket.lastAccessed >= 60 * 60:
		id = randRange(bucket.min, bucket.max)
		self.findNode(id, callback)
	
 
    #####
    ##### UNSOLICITED INCOMING MESSAGE HANDLERS
    
    def _pingHandler(self, t, msg):
	#print "Got PING from %s at %s:%s" % (`t.target.id`, t.target.host, t.target.port)
	self.insertNode(t.target)
	# respond, no callbacks, we don't care if they get it or not
	nt = self.tf.Pong(t)
	nt.dispatch()
	
    def _findNodeHandler(self, t, msg):
	#print "Got FIND_NODES from %s:%s at %s:%s" % (t.target.host, t.target.port, self.node.host, self.node.port)
	nodes = self.table.findNodes(msg['target'])
	# respond, no callbacks, we don't care if they get it or not
	nt = self.tf.GotNodes(t, nodes)
	nt.dispatch()
    
    def _storeValueHandler(self, t, msg):
	if not self.store.has_key(msg['key']):
	    self.store.put(msg['key'], msg['value'])
	nt = self.tf.StoredValue(t)
	nt.dispatch()
    
    def _findValueHandler(self, t, msg):
	if self.store.has_key(msg['key']):
	    t = self.tf.GotValues(t, [(msg['key'], self.store[msg['key']])])
	else:
	    nodes = self.table.findNodes(msg['key'])
	    t = self.tf.GotNodes(t, nodes)
	t.dispatch()


    ###
    ### message response callbacks
    # called when we get a response to store value
    def _storedValueHandler(self, t, msg):
	self.table.insertNode(t.target)


    ## these are the callbacks used when we ping the oldest node in a bucket
    def _staleNodeHandler(self, t):
	""" called if the pinged node never responds """
	self.table.replaceStaleNode(t.target, t.newnode)

    def _notStaleNodeHandler(self, t, msg):
	""" called when we get a ping from the remote node """
	self.table.insertNode(t.target)
	
    
    ## these are the callbacks we use when we issue a PING
    def _pongHandler(self, t, msg):
	#print "Got PONG from %s at %s:%s" % (`msg['id']`, t.target.host, t.target.port)
	n = Node(msg['id'], t.addr[0], t.addr[1])
	self.table.insertNode(n)

    def _defaultPong(self, t):
	# this should probably increment a failed message counter and dump the node if it gets over a threshold
	print "Never got PONG from %s at %s:%s" % (`t.target.id`, t.target.host, t.target.port)
	
    

class ActionBase:
    """ base class for some long running asynchronous proccesses like finding nodes or values """
    def __init__(self, table, dispatcher, target, callback):
	self.table = table
	self.dispatcher = dispatcher
	self.target = target
	self.int = intify(target)
	self.found = {}
	self.queried = {}
	self.answered = {}
	self.callback = callback
	self.outstanding = 0
	self.finished = 0
	
	def sort(a, b, int=self.int):
	    """ this function is for sorting nodes relative to the ID we are looking for """
	    x, y = int ^ a.int, int ^ b.int
	    if x > y:
		return 1
	    elif x < y:
		return -1
	    return 0
	self.sort = sort
    
    def goWithNodes(self, t):
	pass
	
class FindNode(ActionBase):
    """ find node action merits it's own class as it is a long running stateful process """
    def handleGotNodes(self, t, msg):
	if self.finished or self.answered.has_key(t.id):
	    # a day late and a dollar short
	    return
	self.outstanding = self.outstanding - 1
	self.answered[t.id] = 1
	for node in msg['nodes']:
	    if not self.found.has_key(node['id']):
		n = Node(node['id'], node['host'], node['port'])
		self.found[n.id] = n
		self.table.insertNode(n)
	self.schedule()
		
    def schedule(self):
	"""
	    send messages to new peers, if necessary
	"""
	if self.finished:
	    return
	l = self.found.values()
	l.sort(self.sort)

	for node in l[:K]:
	    if node.id == self.target:
		self.finished=1
		return self.callback([node])
	    if not self.queried.has_key(node.id) and node.id != self.table.node.id:
		t = self.table.tf.FindNode(node, self.target, self.handleGotNodes, self.defaultGotNodes)
		self.outstanding = self.outstanding + 1
		self.queried[node.id] = 1
		t.timeout = time.time() + 15
		t.dispatch()
	    if self.outstanding >= N:
		break
	assert(self.outstanding) >=0
	if self.outstanding == 0:
	    ## all done!!
	    self.finished=1
	    self.callback(l[:K])
	
    def defaultGotNodes(self, t):
	if self.finished:
	    return
	self.outstanding = self.outstanding - 1
	self.schedule()
	
	
    def goWithNodes(self, t):
	"""
	    this starts the process, our argument is a transaction with t.extras being our list of nodes
	    it's a transaction since we got called from the dispatcher
	"""
	nodes = t.extras
	for node in nodes:
	    if node.id == self.table.node.id:
		continue
	    self.found[node.id] = node
	    t = self.table.tf.FindNode(node, self.target, self.handleGotNodes, self.defaultGotNodes)
	    t.timeout = time.time() + 15
	    t.dispatch()
	    self.outstanding = self.outstanding + 1
	    self.queried[node.id] = 1
	if self.outstanding == 0:
	    self.callback(nodes)



class GetValue(FindNode):
    """ get value task """
    def handleGotNodes(self, t, msg):
	if self.finished or self.answered.has_key(t.id):
	    # a day late and a dollar short
	    return
	self.outstanding = self.outstanding - 1
	self.answered[t.id] = 1
	# go through nodes
	# if we have any closer than what we already got, query them
	if msg['type'] == 'got nodes':
	    for node in msg['nodes']:
		if not self.found.has_key(node['id']):
		    n = Node(node['id'], node['host'], node['port'])
		    self.found[n.id] = n
		    self.table.insertNode(n)
	elif msg['type'] == 'got values':
	    ## done
	    self.finished = 1
	    return self.callback(msg['values'])
	self.schedule()
		
    ## get value
    def schedule(self):
	if self.finished:
	    return
	l = self.found.values()
	l.sort(self.sort)

	for node in l[:K]:
	    if not self.queried.has_key(node.id) and node.id != self.table.node.id:
		t = self.table.tf.GetValue(node, self.target, self.handleGotNodes, self.defaultGotNodes)
		self.outstanding = self.outstanding + 1
		self.queried[node.id] = 1
		t.timeout = time.time() + 15
		t.dispatch()
	    if self.outstanding >= N:
		break
	assert(self.outstanding) >=0
	if self.outstanding == 0:
	    ## all done, didn't find it!!
	    self.finished=1
	    self.callback([])
    
    ## get value
    def goWithNodes(self, t):
	nodes = t.extras
	for node in nodes:
	    if node.id == self.table.node.id:
		continue
	    self.found[node.id] = node
	    t = self.table.tf.GetValue(node, self.target, self.handleGotNodes, self.defaultGotNodes)
	    t.timeout = time.time() + 15
	    t.dispatch()
	    self.outstanding = self.outstanding + 1
	    self.queried[node.id] = 1
	if self.outstanding == 0:
	    self.callback([])


#------
def test_build_net(quiet=0):
    from whrandom import randrange
    import thread
    port = 2001
    l = []
    peers = 100
    
    if not quiet:
	print "Building %s peer table." % peers
	
    for i in xrange(peers):
	a = Khashmir('localhost', port + i)
	l.append(a)
    
    def run(l=l):
	while(1):
		events = 0
		for peer in l:
			events = events + peer.dispatcher.runOnce()
		if events == 0:
			time.sleep(.25)

    for i in range(10):
	thread.start_new_thread(run, (l[i*10:(i+1)*10],))
	#thread.start_new_thread(l[i].dispatcher.run, ())
    
    for peer in l[1:]:
	n = l[randrange(0, len(l))].node
	peer.addContact(n.host, n.port)
	n = l[randrange(0, len(l))].node
	peer.addContact(n.host, n.port)
	n = l[randrange(0, len(l))].node
	peer.addContact(n.host, n.port)
	
    time.sleep(5)

    for peer in l:
	peer.findCloseNodes()
    time.sleep(5)
    for peer in l:
	peer.refreshTable()
    return l
        
def test_find_nodes(l, quiet=0):
    import threading, sys
    from whrandom import randrange
    flag = threading.Event()
    
    n = len(l)
    
    a = l[randrange(0,n)]
    b = l[randrange(0,n)]
    
    def callback(nodes, l=l, flag=flag):
	if (len(nodes) >0) and (nodes[0].id == b.node.id):
	    print "test_find_nodes	PASSED"
	else:
	    print "test_find_nodes	FAILED"
	flag.set()
    a.findNode(b.node.id, callback)
    flag.wait()
    
def test_find_value(l, quiet=0):
    from whrandom import randrange
    from sha import sha
    import time, threading, sys
    
    fa = threading.Event()
    fb = threading.Event()
    fc = threading.Event()
    
    n = len(l)
    a = l[randrange(0,n)]
    b = l[randrange(0,n)]
    c = l[randrange(0,n)]
    d = l[randrange(0,n)]

    key = sha(`randrange(0,100000)`).digest()
    value = sha(`randrange(0,100000)`).digest()
    if not quiet:
	print "inserting value...",
	sys.stdout.flush()
    a.storeValueForKey(key, value)
    time.sleep(3)
    print "finding..."
    
    def mc(flag, value=value):
	def callback(values, f=flag, val=value):
	    try:
		if(len(values) == 0):
		    print "find                FAILED"
		else:
		    if values[0]['value'] != val:
			print "find                FAILED"
		    else:
			print "find                FOUND"
	    finally:
		f.set()
	return callback
    b.valueForKey(key, mc(fa))
    c.valueForKey(key, mc(fb))
    d.valueForKey(key, mc(fc))
    
    fa.wait()
    fb.wait()
    fc.wait()
    
if __name__ == "__main__":
    l = test_build_net()
    time.sleep(3)
    print "finding nodes..."
    test_find_nodes(l)
    test_find_nodes(l)
    test_find_nodes(l)
    print "inserting and fetching values..."
    test_find_value(l)
    test_find_value(l)
    test_find_value(l)
    test_find_value(l)
    test_find_value(l)
    test_find_value(l)
    for i in l:
	i.dispatcher.stop()
