## Copyright 2002 Andrew Loewenstern, All Rights Reserved

from const import reactor
import const

import time

from sha import sha

from ktable import KTable, K
from knode import KNode as Node

from hash import newID, newIDInRange

from actions import FindNode, GetValue, KeyExpirer
from twisted.web import xmlrpc
from twisted.internet.defer import Deferred
from twisted.python import threadable
from twisted.internet.app import Application
from twisted.web import server
threadable.init()

import sqlite  ## find this at http://pysqlite.sourceforge.net/
import pysqlite_exceptions

KhashmirDBExcept = "KhashmirDBExcept"

# this is the main class!
class Khashmir(xmlrpc.XMLRPC):
	__slots__ = ('listener', 'node', 'table', 'store', 'app', 'last')
	def __init__(self, host, port, db='khashmir.db'):
		self.setup(host, port, db)
	def setup(self, host, port, db='khashmir.db'):
		self.node = Node().init(newID(), host, port)
		self.table = KTable(self.node)
		self.app = Application("xmlrpc")
		self.app.listenTCP(port, server.Site(self))
		self.findDB(db)
		self.last = time.time()
		KeyExpirer(store=self.store)

	def findDB(self, db):
		import os
		try:
			os.stat(db)
		except OSError:
			self.createNewDB(db)
		else:
			self.loadDB(db)
	    
	def loadDB(self, db):
		try:
			self.store = sqlite.connect(db=db)
			self.store.autocommit = 1
		except:
			import traceback
			raise KhashmirDBExcept, "Couldn't open DB", traceback.exc_traceback
	    
	def createNewDB(self, db):
		self.store = sqlite.connect(db=db)
		self.store.autocommit = 1
		s = """
			create table kv (key text, value text, time timestamp, primary key (key, value));
			create index kv_key on kv(key);
			create index kv_timestamp on kv(time);
			
			create table nodes (id text primary key, host text, port number);
			"""
		c = self.store.cursor()
		c.execute(s)

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
		n =Node().init(const.NULL_ID, host, port)  # note, we 
		self.sendPing(n)

	## this call is async!
	def findNode(self, id, callback, errback=None):
		""" returns the contact info for node, or the k closest nodes, from the global table """
		# get K nodes out of local table/cache, or the node we want
		nodes = self.table.findNodes(id)
		d = Deferred()
		if errback:
			d.addCallbacks(callback, errback)
		else:
			d.addCallback(callback)
		if len(nodes) == 1 and nodes[0].id == id :
			d.callback(nodes)
		else:
			# create our search state
			state = FindNode(self, id, d.callback)
			reactor.callFromThread(state.goWithNodes, nodes)
	
	
	## also async
	def valueForKey(self, key, callback):
		""" returns the values found for key in global table
			callback will be called with a list of values for each peer that returns unique values
			final callback will be an empty list - probably should change to 'more coming' arg
		"""
		nodes = self.table.findNodes(key)
		
		# get locals
		l = self.retrieveValues(key)
		if len(l) > 0:
			reactor.callFromThread(callback, map(lambda a: a.decode('base64'), l))
		
		# create our search state
		state = GetValue(self, key, callback)
		reactor.callFromThread(state.goWithNodes, nodes, l)

	## async, callback indicates nodes we got a response from (but no guarantee they didn't drop it on the floor)
	def storeValueForKey(self, key, value, callback=None):
		""" stores the value for key in the global table, returns immediately, no status 
			in this implementation, peers respond but don't indicate status to storing values
			a key can have many values
		"""
		def _storeValueForKey(nodes, key=key, value=value, response=callback , table=self.table):
			if not response:
				# default callback
				def _storedValueHandler(sender):
					pass
				response=_storedValueHandler
		
			for node in nodes[:const.STORE_REDUNDANCY]:
				def cb(t, table = table, node=node, resp=response):
					self.table.insertNode(node)
					response(t)
				if node.id != self.node.id:
					def default(err, node=node, table=table):
						table.nodeFailed(node)
					df = node.storeValue(key, value, self.node.senderDict())
					df.addCallbacks(cb, default)
		# this call is asynch
		self.findNode(key, _storeValueForKey)
	
	
	def insertNode(self, n, contacted=1):
		"""
		insert a node in our local table, pinging oldest contact in bucket, if necessary
		
		If all you have is a host/port, then use addContact, which calls this method after
		receiving the PONG from the remote node.  The reason for the seperation is we can't insert
		a node into the table without it's peer-ID.  That means of course the node passed into this
		method needs to be a properly formed Node object with a valid ID.
		"""
		old = self.table.insertNode(n, contacted=contacted)
		if old and (time.time() - old.lastSeen) > const.MIN_PING_INTERVAL and old.id != self.node.id:
			# the bucket is full, check to see if old node is still around and if so, replace it
			
			## these are the callbacks used when we ping the oldest node in a bucket
			def _staleNodeHandler(oldnode=old, newnode = n):
				""" called if the pinged node never responds """
				self.table.replaceStaleNode(old, newnode)
			
			def _notStaleNodeHandler(sender, old=old):
				""" called when we get a pong from the old node """
				args, sender = sender
				sender = Node().initWithDict(sender)
				if sender.id == old.id:
					self.table.justSeenNode(old)
			
			df = old.ping(self.node.senderDict())
			df.addCallbacks(_notStaleNodeHandler, _staleNodeHandler)

	def sendPing(self, node):
		"""
			ping a node
		"""
		df = node.ping(self.node.senderDict())
		## these are the callbacks we use when we issue a PING
		def _pongHandler(args, id=node.id, host=node.host, port=node.port, table=self.table):
			l, sender = args
			if id != const.NULL_ID and id != sender['id'].decode('base64'):
				# whoah, got response from different peer than we were expecting
				pass
			else:
				sender['host'] = host
				sender['port'] = port
				n = Node().initWithDict(sender)
				table.insertNode(n)
				return
		def _defaultPong(err, node=node, table=self.table):
			table.nodeFailed(node)
		
		df.addCallbacks(_pongHandler,_defaultPong)

	def findCloseNodes(self, callback=lambda a: None):
		"""
			This does a findNode on the ID one away from our own.  
			This will allow us to populate our table with nodes on our network closest to our own.
			This is called as soon as we start up with an empty table
		"""
		id = self.node.id[:-1] + chr((ord(self.node.id[-1]) + 1) % 256)
		self.findNode(id, callback)

	def refreshTable(self):
		"""
			
		"""
		def callback(nodes):
			pass
	
		for bucket in self.table.buckets:
			if time.time() - bucket.lastAccessed >= const.BUCKET_STALENESS:
				id = newIDInRange(bucket.min, bucket.max)
				self.findNode(id, callback)


	def retrieveValues(self, key):
		s = "select value from kv where key = '%s';" % key.encode('base64')
		c = self.store.cursor()
		c.execute(s)
		t = c.fetchone()
		l = []
		while t:
			l.append(t['value'])
			t = c.fetchone()
		return l
	
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
		self.insertNode(n, contacted=0)
		return (), self.node.senderDict()
		
	def xmlrpc_find_node(self, target, sender):
		nodes = self.table.findNodes(target.decode('base64'))
		nodes = map(lambda node: node.senderDict(), nodes)
		ip = self.crequest.getClientIP()
		sender['host'] = ip
		n = Node().initWithDict(sender)
		self.insertNode(n, contacted=0)
		return nodes, self.node.senderDict()
    	    
	def xmlrpc_store_value(self, key, value, sender):
		t = "%0.6f" % time.time()
		s = "insert into kv values ('%s', '%s', '%s');" % (key, value, t)
		c = self.store.cursor()
		try:
			c.execute(s)
		except pysqlite_exceptions.IntegrityError, reason:
			# update last insert time
			s = "update kv set time = '%s' where key = '%s' and value = '%s';" % (t, key, value)
			c.execute(s)
		ip = self.crequest.getClientIP()
		sender['host'] = ip
		n = Node().initWithDict(sender)
		self.insertNode(n, contacted=0)
		return (), self.node.senderDict()
	
	def xmlrpc_find_value(self, key, sender):
		ip = self.crequest.getClientIP()
		key = key.decode('base64')
		sender['host'] = ip
		n = Node().initWithDict(sender)
		self.insertNode(n, contacted=0)
	
		l = self.retrieveValues(key)
		if len(l) > 0:
			return {'values' : l}, self.node.senderDict()
		else:
			nodes = self.table.findNodes(key)
			nodes = map(lambda node: node.senderDict(), nodes)
			return {'nodes' : nodes}, self.node.senderDict()
#------ testing

def test_build_net(quiet=0, peers=24, host='localhost',  pause=0):
	from whrandom import randrange
	import threading
	import thread
	port = 2001
	l = []
		
	if not quiet:
		print "Building %s peer table." % peers
	
	for i in xrange(peers):
		a = Khashmir(host, port + i, db = '/tmp/test'+`i`)
		l.append(a)
	
	
	thread.start_new_thread(l[0].app.run, ())
	time.sleep(1)
	for peer in l[1:]:
		peer.app.run()
	time.sleep(10)
	
	print "adding contacts...."
	
	for peer in l[1:]:
		n = l[randrange(0, len(l))].node
		peer.addContact(host, n.port)
		n = l[randrange(0, len(l))].node
		peer.addContact(host, n.port)
		n = l[randrange(0, len(l))].node
		peer.addContact(host, n.port)
	if pause:
		time.sleep(.33)
	
	time.sleep(10)
	print "finding close nodes...."
	
	for peer in l:
		flag = threading.Event()
		def cb(nodes, f=flag):
			f.set()
		peer.findCloseNodes(cb)
		flag.wait()
	
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
	if not quiet:
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
						print "find                NOT FOUND"
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
    
def test_one(host, port, db='/tmp/test'):
	import thread
	k = Khashmir(host, port, db)
	thread.start_new_thread(k.app.run, ())
	return k
    
if __name__ == "__main__":
    import sys
    n = 8
    if len(sys.argv) > 1:
		n = int(sys.argv[1])
    l = test_build_net(peers=n)
    time.sleep(3)
    print "finding nodes..."
    for i in range(10):
		test_find_nodes(l)
    print "inserting and fetching values..."
    for i in range(10):
		test_find_value(l)
