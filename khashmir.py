## Copyright 2002 Andrew Loewenstern, All Rights Reserved

from const import reactor
import time
from pickle import loads, dumps
from sha import sha

from ktable import KTable, K
from knode import KNode as Node

from hash import newID

from actions import FindNode, GetValue, KeyExpirer
from twisted.web import xmlrpc
from twisted.internet.defer import Deferred
from twisted.python import threadable
from twisted.internet.app import Application
from twisted.web import server
threadable.init()

from bsddb3 import db ## find this at http://pybsddb.sf.net/
from bsddb3._db import DBNotFoundError

from xmlrpclib import Binary

# don't ping unless it's been at least this many seconds since we've heard from a peer
MAX_PING_INTERVAL = 60 * 15 # fifteen minutes



# this is the main class!
class Khashmir(xmlrpc.XMLRPC):
    __slots__ = ['listener', 'node', 'table', 'store', 'itime', 'kw', 'app']
    def __init__(self, host, port):
	self.node = Node().init(newID(), host, port)
	self.table = KTable(self.node)
	self.app = Application("xmlrpc")
	self.app.listenTCP(port, server.Site(self))
	
	## these databases may be more suited to on-disk rather than in-memory
	# h((key, value)) -> (key, value, time) mappings
	self.store = db.DB()
	self.store.open(None, None, db.DB_BTREE)
	
	# <insert time> -> h((key, value))
	self.itime = db.DB()
	self.itime.set_flags(db.DB_DUP)
	self.itime.open(None, None, db.DB_BTREE)

	# key -> h((key, value))
	self.kw = db.DB()
	self.kw.set_flags(db.DB_DUP)
	self.kw.open(None, None, db.DB_BTREE)

	KeyExpirer(store=self.store, itime=self.itime, kw=self.kw)
	
    def render(self, request):
	"""
	    Override the built in render so we can have access to the request object!
	    note, crequest is probably only valid on the initial call (not after deferred!)
	"""
	self.crequest = request
	return xmlrpc.XMLRPC.render(self, request)

	
    #######
    #######  LOCAL INTERFACE    - use these methods!
    def addContact(self, host, port):
	"""
	 ping this node and add the contact info to the table on pong!
	"""
	n =Node().init(" "*20, host, port)  # note, we 
	self.sendPing(n)


    ## this call is async!
    def findNode(self, id, callback, errback=None):
	""" returns the contact info for node, or the k closest nodes, from the global table """
	# get K nodes out of local table/cache, or the node we want
	nodes = self.table.findNodes(id)
	d = Deferred()
	d.addCallbacks(callback, errback)
	if len(nodes) == 1 and nodes[0].id == id :
	    d.callback(nodes)
	else:
	    # create our search state
	    state = FindNode(self, id, d.callback)
	    reactor.callFromThread(state.goWithNodes, nodes)
    
    
    ## also async
    def valueForKey(self, key, callback):
	""" returns the values found for key in global table """
	nodes = self.table.findNodes(key)
	# decode values, they will be base64 encoded
	# create our search state
	state = GetValue(self, key, callback)
	reactor.callFromThread(state.goWithNodes, nodes)



    ## async, but in the current implementation there is no guarantee a store does anything so there is no callback right now
    def storeValueForKey(self, key, value, callback=None):
	""" stores the value for key in the global table, returns immediately, no status 
	    in this implementation, peers respond but don't indicate status to storing values
	    values are stored in peers on a first-come first-served basis
	    this will probably change so more than one value can be stored under a key
	"""
	def _storeValueForKey(nodes, key=key, value=value, response=callback , default= lambda t: "didn't respond"):
	    if not callback:
	        # default callback - this will get called for each successful store value
		def _storedValueHandler(sender):
		    pass
		response=_storedValueHandler
	    for node in nodes:
		if node.id != self.node.id:
		    df = node.storeValue(key, value, self.node.senderDict())
		    df.addCallbacks(response, default)
	# this call is asynch
	self.findNode(key, _storeValueForKey)
	
	
    def insertNode(self, n):
	"""
	insert a node in our local table, pinging oldest contact in bucket, if necessary
	
	If all you have is a host/port, then use addContact, which calls this method after
	receiving the PONG from the remote node.  The reason for the seperation is we can't insert
	a node into the table without it's peer-ID.  That means of course the node passed into this
	method needs to be a properly formed Node object with a valid ID.
	"""
	old = self.table.insertNode(n)
	if old and (time.time() - old.lastSeen) > MAX_PING_INTERVAL and old.id != self.node.id:
	    # the bucket is full, check to see if old node is still around and if so, replace it
	    
	    ## these are the callbacks used when we ping the oldest node in a bucket
	    def _staleNodeHandler(oldnode=old, newnode = n):
		""" called if the pinged node never responds """
		self.table.replaceStaleNode(old, newnode)
	
	    def _notStaleNodeHandler(sender, old=old):
		""" called when we get a ping from the remote node """
		sender = Node().initWithSenderDict(sender)
		if sender.id == old.id:
		    self.table.insertNode(old)

	    df = old.ping(self.node.senderDict())
	    df.addCallbacks(_notStaleNodeHandler, _staleNodeHandler)


    def sendPing(self, node):
	"""
	    ping a node
	"""
	df = node.ping(self.node.senderDict())
	## these are the callbacks we use when we issue a PING
	def _pongHandler(sender, id=node.id, host=node.host, port=node.port, table=self.table):
	    sender = Node().initWithSenderDict(sender)
	    if id != 20 * ' ' and id != sender.id:
		# whoah, got response from different peer than we were expecting
		pass
	    else:
		#print "Got PONG from %s at %s:%s" % (`msg['id']`, t.target.host, t.target.port)
		sender['host'] = host
		sender['port'] = port
		n = Node().initWithDict(sender)
		table.insertNode(n)
	    return
	def _defaultPong(err):
	    # this should probably increment a failed message counter and dump the node if it gets over a threshold
	    return	

	df.addCallbacks(_pongHandler,_defaultPong)


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
    ##### INCOMING MESSAGE HANDLERS
    
    def xmlrpc_ping(self, sender):
	"""
	    takes sender dict = {'id', <id>, 'port', port} optional keys = 'ip'
	    returns sender dict
	"""
	ip = self.crequest.getClientIP()
	sender['host'] = ip
	n = Node().initWithDict(sender)
	self.insertNode(n)
	return self.node.senderDict()
		
    def xmlrpc_find_node(self, target, sender):
	nodes = self.table.findNodes(target)
	nodes = map(lambda node: node.senderDict(), nodes)
	ip = self.crequest.getClientIP()
	sender['host'] = ip
	n = Node().initWithDict(sender)
	self.insertNode(n)
	return nodes, self.node.senderDict()
    
    def xmlrpc_store_value(self, key, value, sender):
	key = key.data
	h1 = sha(key+value.data).digest()
	t = `time.time()`
	if not self.store.has_key(h1):
	    v = dumps((key, value.data, t))
	    self.store.put(h1, v)
	    self.itime.put(t, h1)
	    self.kw.put(key, h1)
	else:
	    # update last insert time
	    tup = loads(self.store[h1])
	    self.store[h1] = dumps((tup[0], tup[1], t))
	    self.itime.put(t, h1)

	ip = self.crequest.getClientIP()
	sender['host'] = ip
	n = Node().initWithDict(sender)
	self.insertNode(n)
	return self.node.senderDict()
	
    def xmlrpc_find_value(self, key, sender):
    	ip = self.crequest.getClientIP()
	key = key.data
	sender['host'] = ip
	n = Node().initWithDict(sender)
	self.insertNode(n)

	if self.kw.has_key(key):
	    c = self.kw.cursor()
	    tup = c.set(key)
	    l = []
	    while(tup):
		h1 = tup[1]
		v = loads(self.store[h1])[1]
		l.append(v)
		tup = c.next()
	    l = map(lambda v: Binary(v), l)
	    return {'values' : l}, self.node.senderDict()
	else:
	    nodes = self.table.findNodes(key)
	    nodes = map(lambda node: node.senderDict(), nodes)
	    return {'nodes' : nodes}, self.node.senderDict()





#------ testing

def test_build_net(quiet=0, peers=24, host='localhost',  pause=1):
    from whrandom import randrange
    import thread
    port = 2001
    l = []
        
    if not quiet:
	print "Building %s peer table." % peers
	
    for i in xrange(peers):
	a = Khashmir(host, port + i)
	l.append(a)
    

    thread.start_new_thread(l[0].app.run, ())
    time.sleep(1)
    for peer in l[1:]:
	peer.app.run()
	#time.sleep(.25)

    print "adding contacts...."

    for peer in l[1:]:
	n = l[randrange(0, len(l))].node
	peer.addContact(host, n.port)
	n = l[randrange(0, len(l))].node
	peer.addContact(host, n.port)
	n = l[randrange(0, len(l))].node
	peer.addContact(host, n.port)
	if pause:
	    time.sleep(.30)
	    
    time.sleep(1)
    print "finding close nodes...."

    for peer in l:
	peer.findCloseNodes()
	if pause:
	    time.sleep(.5)
    if pause:
	    time.sleep(2)
#    for peer in l:
#	peer.refreshTable()
    return l
        
def test_find_nodes(l, quiet=0):
    import threading, sys
    from whrandom import randrange
    flag = threading.Event()
    
    n = len(l)
    
    a = l[randrange(0,n)]
    b = l[randrange(0,n)]
    
    def callback(nodes, flag=flag, id = b.node.id):
	if (len(nodes) >0) and (nodes[0].id == id):
	    print "test_find_nodes	PASSED"
	else:
	    print "test_find_nodes	FAILED"
	flag.set()
    a.findNode(b.node.id, callback)
    flag.wait()
    
def test_find_value(l, quiet=0):
    from whrandom import randrange
    from sha import sha
    from hash import newID
    import time, threading, sys
    
    fa = threading.Event()
    fb = threading.Event()
    fc = threading.Event()
    
    n = len(l)
    a = l[randrange(0,n)]
    b = l[randrange(0,n)]
    c = l[randrange(0,n)]
    d = l[randrange(0,n)]

    key = newID()
    value = newID()
    if not quiet:
	print "inserting value..."
	sys.stdout.flush()
    a.storeValueForKey(key, value)
    time.sleep(3)
    print "finding..."
    sys.stdout.flush()
    
    class cb:
	def __init__(self, flag, value=value):
	    self.flag = flag
	    self.val = value
	    self.found = 0
	def callback(self, values):
	    try:
		if(len(values) == 0):
		    if not self.found:
			print "find                FAILED"
		    else:
			print "find                FOUND"
		    sys.stdout.flush()

		else:
		    if self.val in values:
			self.found = 1
	    finally:
		self.flag.set()

    b.valueForKey(key, cb(fa).callback)
    fa.wait()
    c.valueForKey(key, cb(fb).callback)
    fb.wait()
    d.valueForKey(key, cb(fc).callback)    
    fc.wait()
    
def test_one(port):
    import thread
    k = Khashmir('localhost', port)
    thread.start_new_thread(k.app.run, ())
    return k
    
if __name__ == "__main__":
    l = test_build_net()
    time.sleep(3)
    print "finding nodes..."
    for i in range(10):
	test_find_nodes(l)
    print "inserting and fetching values..."
    for i in range(10):
	test_find_value(l)
