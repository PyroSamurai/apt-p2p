## Copyright 2002 Andrew Loewenstern, All Rights Reserved

from bsddb3 import db ## find this at http://pybsddb.sf.net/
from bsddb3._db import DBNotFoundError
import time
import hash
from node import Node
from bencode import bencode, bdecode
#from threading import RLock

# max number of incoming or outgoing messages to process at a time
NUM_EVENTS = 5

class Transaction:
    __slots__ = ['responseTemplate', 'id', 'dispatcher', 'target', 'payload', 'response', 'default', 'timeout']
    def __init__(self, dispatcher, node, response_handler, default_handler, id = None, payload = None, timeout=60):
	if id == None:
	    id = hash.newID()
	self.id = id
	self.dispatcher = dispatcher
	self.target = node
	self.payload = payload
	self.response = response_handler
	self.default = default_handler
	self.timeout = time.time() + timeout
	
    def setPayload(self, payload):
	self.payload = payload
	
    def setResponseTemplate(self, t):
	self.responseTemplate = t

    def responseHandler(self, msg):
	if self.responseTemplate and callable(self.responseTemplate):
	    try:
		self.responseTemplate(msg)
	    except ValueError, reason:
		print "response %s" % (reason)
		print `msg['id'], self.target.id`
		return
	self.response(self, msg)
	
    def defaultHandler(self):
	self.default(self)
	
    def dispatch(self):
	if callable(self.response) and callable(self.default):
	    self.dispatcher.initiate(self)
	else:
	    self.dispatchNoResponse()
    def dispatchNoResponse(self):
	self.dispatcher.initiateNoResponse(self)
	
	
	
class Dispatcher:
    def __init__(self, listener, base_template, id):
	self.id = id
	self.listener = listener
	self.transactions = {}
	self.handlers = {}
	self.timeout = db.DB()
	self.timeout.set_flags(db.DB_DUP)
	self.timeout.open(None, None, db.DB_BTREE)
	self.BASE = base_template
	self.stopped = 0
	#self.tlock = RLock()
	
    def registerHandler(self, key, handler, template):
	assert(callable(handler))
	assert(callable(template))
	self.handlers[key] = (handler, template)
	
    def initiate(self, transaction):
	#self.tlock.acquire()
	#ignore messages to ourself
	if transaction.target.id == self.id:
	    return
	self.transactions[transaction.id] = transaction
	self.timeout.put(`transaction.timeout`, transaction.id)
	## queue the message!
	self.listener.qMsg(transaction.payload, transaction.target.host, transaction.target.port)
	#self.tlock.release()
	
    def initiateNoResponse(self, transaction):
    	#ignore messages to ourself
	if transaction.target.id == self.id:
	    return
	#self.tlock.acquire()
	self.listener.qMsg(transaction.payload, transaction.target.host, transaction.target.port)
	#self.tlock.release()
	
    def postEvent(self, callback, delay, extras=None):
	#self.tlock.acquire()
	t = Transaction(self, None, None, callback, timeout=delay)
	t.extras = extras
	self.transactions[t.id] = t
	self.timeout.put(`t.timeout`, t.id)
	#self.tlock.release()
	
    def flushExpiredEvents(self):
	events = 0
	tstamp = `time.time()`
	#self.tlock.acquire()
	c = self.timeout.cursor()
	e = c.first()
	while e and e[0] < tstamp:
	    events = events + 1
	    try:
		t = self.transactions[e[1]]
		del(self.transactions[e[1]])
	    except KeyError:
		# transaction must have completed or was otherwise cancelled
		pass
	    ## default callback!
	    else:
		t.defaultHandler()
	    tmp = c.next()
	    # handle duplicates in a silly way
	    if tmp and e != tmp:
		self.timeout.delete(e[0])
	    e = tmp
	#self.tlock.release()
	return events
	
    def flushOutgoing(self):
	events = 0
	n = self.listener.qLen()
	if n > NUM_EVENTS:
	    n = NUM_EVENTS
	for i in range(n):
	    self.listener.dispatchMsg()
	    events = events + 1
	return events
    
    def handleIncoming(self):
	events = 0
	#self.tlock.acquire()
	for i in range(NUM_EVENTS):
	    try:
		msg, addr = self.listener.receiveMsg()
	    except ValueError:
		break

	    ## decode message, handle message!
	    try:
		msg = bdecode(msg)
	    except ValueError:
		# wrongly encoded message?
		print "Bogus message received: %s" % msg
		continue
	    try:
		# check base template for correctness
		self.BASE(msg)
	    except ValueError, reason:
		# bad message!
		print "Incoming message: %s" % reason
		continue
	    try:
		# check to see if we already know about this transaction
		t = self.transactions[msg['tid']]
		if msg['id'] != t.target.id and t.target.id != " "*20:
		    # we're expecting a response from someone else
		    if msg['id'] == self.id:
			print "received our own response!  " + `self.id`
		    else:
			print "response from wrong peer!  "+ `msg['id'],t.target.id`
		else:
		    del(self.transactions[msg['tid']])
		    self.timeout.delete(`t.timeout`)
		    t.addr = addr
		    # call transaction response handler
		    t.responseHandler(msg)
	    except KeyError:
		# we don't know about it, must be unsolicited
		n = Node(msg['id'], addr[0], addr[1])
		t = Transaction(self, n, None, None, msg['tid'])
		if self.handlers.has_key(msg['type']):
		    ## handle this transaction
		    try:
			# check handler template
			self.handlers[msg['type']][1](msg)
		    except ValueError, reason:
			print "BAD MESSAGE: %s" % reason
		    else:
			self.handlers[msg['type']][0](t, msg)
		else:
		    ## no transaction, no handler, drop it on the floor!
		    pass
	    events = events + 1
	#self.tlock.release()
	return events
	
    def stop(self):
	self.stopped = 1
	
    def run(self):
	self.stopped = 0
	while(not self.stopped):
	    events = self.runOnce()
	    ## sleep
	    if events == 0:
		time.sleep(0.1)
	
    def runOnce(self):
	events = 0
	## handle some incoming messages
	events = events + self.handleIncoming()
	## process some outstanding events
	events = events + self.flushExpiredEvents()
	## send outgoing messages
	events = events + self.flushOutgoing()
	return events



