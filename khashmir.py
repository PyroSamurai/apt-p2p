## Copyright 2002 Andrew Loewenstern, All Rights Reserved

from const import reactor
import const

import time

from sha import sha

from ktable import KTable, K
from knode import KNode as Node

from hash import newID, newIDInRange

from actions import FindNode, GetValue, KeyExpirer, StoreValue
import krpc
import airhook

from twisted.internet.defer import Deferred
from twisted.internet import protocol
from twisted.python import threadable
from twisted.internet.app import Application
from twisted.web import server
threadable.init()
import sys

import sqlite  ## find this at http://pysqlite.sourceforge.net/
import pysqlite_exceptions

class KhashmirDBExcept(Exception):
    pass

# this is the main class!
class Khashmir(protocol.Factory):
    __slots__ = ('listener', 'node', 'table', 'store', 'app', 'last', 'protocol')
    protocol = krpc.KRPC
    def __init__(self, host, port, db='khashmir.db'):
        self.setup(host, port, db)
        
    def setup(self, host, port, db='khashmir.db'):
        self._findDB(db)
        self.node = self._loadSelfNode(host, port)
        self.table = KTable(self.node)
        self.app = Application("krpc")
        self.airhook = airhook.listenAirhookStream(port, self)
        self.last = time.time()
        self._loadRoutingTable()
        KeyExpirer(store=self.store)
        #self.refreshTable(force=1)
        reactor.callLater(60, self.checkpoint, (1,))
        
    def _loadSelfNode(self, host, port):
        c = self.store.cursor()
        c.execute('select id from self where num = 0;')
        if c.rowcount > 0:
            id = c.fetchone()[0].decode('hex')
        else:
            id = newID()
        return Node().init(id, host, port)
        
    def _saveSelfNode(self):
        self.store.autocommit = 0
        c = self.store.cursor()
        c.execute('delete from self where num = 0;')
        c.execute("insert into self values (0, '%s');" % self.node.id.encode('hex'))
        self.store.commit()
        self.store.autocommit = 1
        
    def checkpoint(self, auto=0):
        self._saveSelfNode()
        self._dumpRoutingTable()
        if auto:
            reactor.callLater(const.CHECKPOINT_INTERVAL, self.checkpoint)
        
    def _findDB(self, db):
        import os
        try:
            os.stat(db)
        except OSError:
            self._createNewDB(db)
        else:
            self._loadDB(db)
        
    def _loadDB(self, db):
        try:
            self.store = sqlite.connect(db=db)
            self.store.autocommit = 1
        except:
            import traceback
            raise KhashmirDBExcept, "Couldn't open DB", traceback.exc_traceback
        
    def _createNewDB(self, db):
        self.store = sqlite.connect(db=db)
        self.store.autocommit = 1
        s = """
            create table kv (key text, value text, time timestamp, primary key (key, value));
            create index kv_key on kv(key);
            create index kv_timestamp on kv(time);
            
            create table nodes (id text primary key, host text, port number);
            
            create table self (num number primary key, id text);
            """
        c = self.store.cursor()
        c.execute(s)

    def _dumpRoutingTable(self):
        """
            save routing table nodes to the database
        """
        self.store.autocommit = 0;
        c = self.store.cursor()
        c.execute("delete from nodes where id not NULL;")
        for bucket in self.table.buckets:
            for node in bucket.l:
                d = node.senderDict()
                c.execute("insert into nodes values ('%s', '%s', '%s');" % (d['id'].encode('hex'), d['host'], d['port']))
        self.store.commit()
        self.store.autocommit = 1;
        
    def _loadRoutingTable(self):
        """
            load routing table nodes from database
            it's usually a good idea to call refreshTable(force=1) after loading the table
        """
        c = self.store.cursor()
        c.execute("select * from nodes;")
        for rec in c.fetchall():
            n = Node().initWithDict({'id':rec[0].decode('hex'), 'host':rec[1], 'port':int(rec[2])})
            n.conn = self.airhook.connectionForAddr((n.host, n.port))
            self.table.insertNode(n, contacted=0)
            

    #######
    #######  LOCAL INTERFACE    - use these methods!
    def addContact(self, host, port, callback=None):
        """
            ping this node and add the contact info to the table on pong!
        """
        n =Node().init(const.NULL_ID, host, port) 
        n.conn = self.airhook.connectionForAddr((n.host, n.port))
        self.sendPing(n, callback=callback)

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
            action = StoreValue(self.table, key, value, response)
            reactor.callFromThread(action.goWithNodes, nodes)
            
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
            
            def _notStaleNodeHandler(dict, old=old):
                """ called when we get a pong from the old node """
                sender = dict['sender']
                if sender['id'] == old.id:
                    self.table.justSeenNode(old.id)
            
            df = old.ping(self.node.senderDict())
            df.addCallbacks(_notStaleNodeHandler, _staleNodeHandler)

    def sendPing(self, node, callback=None):
        """
            ping a node
        """
        df = node.ping(self.node.senderDict())
        ## these are the callbacks we use when we issue a PING
        def _pongHandler(dict, node=node, table=self.table, callback=callback):
            sender = dict['sender']
            if node.id != const.NULL_ID and node.id != sender['id']:
                # whoah, got response from different peer than we were expecting
                self.table.invalidateNode(node)
            else:
                sender['host'] = node.host
                sender['port'] = node.port
                n = Node().initWithDict(sender)
                n.conn = self.airhook.connectionForAddr((n.host, n.port))
                table.insertNode(n)
                if callback:
                    callback()
        def _defaultPong(err, node=node, table=self.table, callback=callback):
            table.nodeFailed(node)
            if callback:
                callback()
        
        df.addCallbacks(_pongHandler,_defaultPong)

    def findCloseNodes(self, callback=lambda a: None):
        """
            This does a findNode on the ID one away from our own.  
            This will allow us to populate our table with nodes on our network closest to our own.
            This is called as soon as we start up with an empty table
        """
        id = self.node.id[:-1] + chr((ord(self.node.id[-1]) + 1) % 256)
        self.findNode(id, callback)

    def refreshTable(self, force=0):
        """
            force=1 will refresh table regardless of last bucket access time
        """
        def callback(nodes):
            pass
    
        for bucket in self.table.buckets:
            if force or (time.time() - bucket.lastAccessed >= const.BUCKET_STALENESS):
                id = newIDInRange(bucket.min, bucket.max)
                self.findNode(id, callback)


    def retrieveValues(self, key):
        s = "select value from kv where key = '%s';" % key.encode('hex')
        c = self.store.cursor()
        c.execute(s)
        t = c.fetchone()
        l = []
        while t:
            l.append(t['value'].decode('base64'))
            t = c.fetchone()
        return l
    
    #####
    ##### INCOMING MESSAGE HANDLERS
    
    def krpc_ping(self, sender, _krpc_sender):
        """
            takes sender dict = {'id', <id>, 'port', port} optional keys = 'ip'
            returns sender dict
        """
        sender['host'] = _krpc_sender[0]
        n = Node().initWithDict(sender)
        n.conn = self.airhook.connectionForAddr((n.host, n.port))
        self.insertNode(n, contacted=0)
        return {"sender" : self.node.senderDict()}
        
    def krpc_find_node(self, target, sender, _krpc_sender):
        nodes = self.table.findNodes(target)
        nodes = map(lambda node: node.senderDict(), nodes)
        sender['host'] = _krpc_sender[0]
        n = Node().initWithDict(sender)
        n.conn = self.airhook.connectionForAddr((n.host, n.port))
        self.insertNode(n, contacted=0)
        return {"nodes" : nodes, "sender" : self.node.senderDict()}
            
    def krpc_store_value(self, key, value, sender, _krpc_sender):
        t = "%0.6f" % time.time()
        s = "insert into kv values ('%s', '%s', '%s');" % (key.encode("hex"), value.encode("base64"), t)
        c = self.store.cursor()
        try:
            c.execute(s)
        except pysqlite_exceptions.IntegrityError, reason:
            # update last insert time
            s = "update kv set time = '%s' where key = '%s' and value = '%s';" % (t, key.encode("hex"), value.encode("base64"))
            c.execute(s)
        sender['host'] = _krpc_sender[0]
        n = Node().initWithDict(sender)
        n.conn = self.airhook.connectionForAddr((n.host, n.port))
        self.insertNode(n, contacted=0)
        return {"sender" : self.node.senderDict()}
    
    def krpc_find_value(self, key, sender, _krpc_sender):
        sender['host'] = _krpc_sender[0]
        n = Node().initWithDict(sender)
        n.conn = self.airhook.connectionForAddr((n.host, n.port))
        self.insertNode(n, contacted=0)
    
        l = self.retrieveValues(key)
        if len(l) > 0:
            return {'values' : l, "sender": self.node.senderDict()}
        else:
            nodes = self.table.findNodes(key)
            nodes = map(lambda node: node.senderDict(), nodes)
            return {'nodes' : nodes, "sender": self.node.senderDict()}

### TESTING ###
from random import randrange
import threading, thread, sys, time
from sha import sha
from hash import newID


def test_net(peers=24, startport=2001, dbprefix='/tmp/test'):
    import thread
    l = []
    for i in xrange(peers):
        a = Khashmir('127.0.0.1', startport + i, db = dbprefix+`i`)
        l.append(a)
    thread.start_new_thread(l[0].app.run, ())
    for peer in l[1:]:
        peer.app.run()	
    return l
    
def test_build_net(quiet=0, peers=24, host='127.0.0.1',  pause=0, startport=2001, dbprefix='/tmp/test'):
    from whrandom import randrange
    import threading
    import thread
    import sys
    port = startport
    l = []
    if not quiet:
        print "Building %s peer table." % peers
    
    for i in xrange(peers):
        a = Khashmir(host, port + i, db = dbprefix +`i`)
        l.append(a)
    
    
    thread.start_new_thread(l[0].app.run, ())
    time.sleep(1)
    for peer in l[1:]:
        peer.app.run()
    #time.sleep(3)
    
    def spewer(frame, s, ignored):
        from twisted.python import reflect
        if frame.f_locals.has_key('self'):
            se = frame.f_locals['self']
            print 'method %s of %s at %s' % (
                frame.f_code.co_name, reflect.qual(se.__class__), id(se)
                )
    #sys.settrace(spewer)

    print "adding contacts...."
    def makecb(flag):
        def cb(f=flag):
            f.set()
        return cb

    for peer in l:
        p = l[randrange(0, len(l))]
        if p != peer:
            n = p.node
            flag = threading.Event()
            peer.addContact(host, n.port, makecb(flag))
            flag.wait()
        p = l[randrange(0, len(l))]
        if p != peer:
            n = p.node
            flag = threading.Event()
            peer.addContact(host, n.port, makecb(flag))
            flag.wait()
        p = l[randrange(0, len(l))]
        if p != peer:
            n = p.node
            flag = threading.Event()
            peer.addContact(host, n.port, makecb(flag))
            flag.wait()
    
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
    if not quiet: print "inserting value..."
    a.storeValueForKey(key, value)
    time.sleep(3)
    if not quiet:
        print "finding..."
    
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
    thread.start_new_thread(reactor.run, ())
    return k
    
if __name__ == "__main__":
    import sys
    n = 8
    if len(sys.argv) > 1: n = int(sys.argv[1])
    l = test_build_net(peers=n)
    time.sleep(3)
    print "finding nodes..."
    for i in range(10):
        test_find_nodes(l)
    print "inserting and fetching values..."
    for i in range(10):
        test_find_value(l)
