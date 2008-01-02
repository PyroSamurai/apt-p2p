## Copyright 2002-2004 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

import warnings
warnings.simplefilter("ignore", DeprecationWarning)

from time import time
from random import randrange
from sha import sha
import os
import sqlite  ## find this at http://pysqlite.sourceforge.net/

from twisted.internet.defer import Deferred
from twisted.internet import protocol, reactor
from twisted.trial import unittest

from ktable import KTable
from knode import KNodeBase, KNodeRead, KNodeWrite, NULL_ID
from khash import newID, newIDInRange
from actions import FindNode, GetValue, KeyExpirer, StoreValue
import krpc

class KhashmirDBExcept(Exception):
    pass

# this is the base class, has base functionality and find node, no key-value mappings
class KhashmirBase(protocol.Factory):
    _Node = KNodeBase
    def __init__(self, config, cache_dir='/tmp'):
        self.config = None
        self.setup(config, cache_dir)
        
    def setup(self, config, cache_dir):
        self.config = config
        self.port = config['PORT']
        self._findDB(os.path.join(cache_dir, 'khashmir.' + str(self.port) + '.db'))
        self.node = self._loadSelfNode('', self.port)
        self.table = KTable(self.node, config)
        #self.app = service.Application("krpc")
        self.udp = krpc.hostbroker(self)
        self.udp.protocol = krpc.KRPC
        self.listenport = reactor.listenUDP(self.port, self.udp)
        self.last = time()
        self._loadRoutingTable()
        self.expirer = KeyExpirer(self.store, config)
        self.refreshTable(force=1)
        self.next_checkpoint = reactor.callLater(60, self.checkpoint, (1,))

    def Node(self):
        n = self._Node()
        n.table = self.table
        return n
    
    def __del__(self):
        self.listenport.stopListening()
        
    def _loadSelfNode(self, host, port):
        c = self.store.cursor()
        c.execute('select id from self where num = 0;')
        if c.rowcount > 0:
            id = c.fetchone()[0]
        else:
            id = newID()
        return self._Node().init(id, host, port)
        
    def _saveSelfNode(self):
        c = self.store.cursor()
        c.execute('delete from self where num = 0;')
        c.execute("insert into self values (0, %s);", sqlite.encode(self.node.id))
        self.store.commit()
        
    def checkpoint(self, auto=0):
        self._saveSelfNode()
        self._dumpRoutingTable()
        self.refreshTable()
        if auto:
            self.next_checkpoint = reactor.callLater(randrange(int(self.config['CHECKPOINT_INTERVAL'] * .9), 
                                        int(self.config['CHECKPOINT_INTERVAL'] * 1.1)), 
                              self.checkpoint, (1,))
        
    def _findDB(self, db):
        self.db = db
        try:
            os.stat(db)
        except OSError:
            self._createNewDB(db)
        else:
            self._loadDB(db)
        
    def _loadDB(self, db):
        try:
            self.store = sqlite.connect(db=db)
            #self.store.autocommit = 0
        except:
            import traceback
            raise KhashmirDBExcept, "Couldn't open DB", traceback.format_exc()
        
    def _createNewDB(self, db):
        self.store = sqlite.connect(db=db)
        s = """
            create table kv (key binary, value binary, time timestamp, primary key (key, value));
            create index kv_key on kv(key);
            create index kv_timestamp on kv(time);
            
            create table nodes (id binary primary key, host text, port number);
            
            create table self (num number primary key, id binary);
            """
        c = self.store.cursor()
        c.execute(s)
        self.store.commit()

    def _dumpRoutingTable(self):
        """
            save routing table nodes to the database
        """
        c = self.store.cursor()
        c.execute("delete from nodes where id not NULL;")
        for bucket in self.table.buckets:
            for node in bucket.l:
                c.execute("insert into nodes values (%s, %s, %s);", (sqlite.encode(node.id), node.host, node.port))
        self.store.commit()
        
    def _loadRoutingTable(self):
        """
            load routing table nodes from database
            it's usually a good idea to call refreshTable(force=1) after loading the table
        """
        c = self.store.cursor()
        c.execute("select * from nodes;")
        for rec in c.fetchall():
            n = self.Node().initWithDict({'id':rec[0], 'host':rec[1], 'port':int(rec[2])})
            n.conn = self.udp.connectionForAddr((n.host, n.port))
            self.table.insertNode(n, contacted=0)
            

    #######
    #######  LOCAL INTERFACE    - use these methods!
    def addContact(self, host, port, callback=None):
        """
            ping this node and add the contact info to the table on pong!
        """
        n =self.Node().init(NULL_ID, host, port) 
        n.conn = self.udp.connectionForAddr((n.host, n.port))
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
            state = FindNode(self, id, d.callback, self.config)
            reactor.callLater(0, state.goWithNodes, nodes)
    
    def insertNode(self, n, contacted=1):
        """
        insert a node in our local table, pinging oldest contact in bucket, if necessary
        
        If all you have is a host/port, then use addContact, which calls this method after
        receiving the PONG from the remote node.  The reason for the seperation is we can't insert
        a node into the table without it's peer-ID.  That means of course the node passed into this
        method needs to be a properly formed Node object with a valid ID.
        """
        old = self.table.insertNode(n, contacted=contacted)
        if old and (time() - old.lastSeen) > self.config['MIN_PING_INTERVAL'] and old.id != self.node.id:
            # the bucket is full, check to see if old node is still around and if so, replace it
            
            ## these are the callbacks used when we ping the oldest node in a bucket
            def _staleNodeHandler(oldnode=old, newnode = n):
                """ called if the pinged node never responds """
                self.table.replaceStaleNode(old, newnode)
            
            def _notStaleNodeHandler(dict, old=old):
                """ called when we get a pong from the old node """
                dict = dict['rsp']
                if dict['id'] == old.id:
                    self.table.justSeenNode(old.id)
            
            df = old.ping(self.node.id)
            df.addCallbacks(_notStaleNodeHandler, _staleNodeHandler)

    def sendPing(self, node, callback=None):
        """
            ping a node
        """
        df = node.ping(self.node.id)
        ## these are the callbacks we use when we issue a PING
        def _pongHandler(dict, node=node, table=self.table, callback=callback):
            _krpc_sender = dict['_krpc_sender']
            dict = dict['rsp']
            sender = {'id' : dict['id']}
            sender['host'] = _krpc_sender[0]
            sender['port'] = _krpc_sender[1]
            n = self.Node().initWithDict(sender)
            n.conn = self.udp.connectionForAddr((n.host, n.port))
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
            if force or (time() - bucket.lastAccessed >= self.config['BUCKET_STALENESS']):
                id = newIDInRange(bucket.min, bucket.max)
                self.findNode(id, callback)

    def stats(self):
        """
        Returns (num_contacts, num_nodes)
        num_contacts: number contacts in our routing table
        num_nodes: number of nodes estimated in the entire dht
        """
        num_contacts = reduce(lambda a, b: a + len(b.l), self.table.buckets, 0)
        num_nodes = self.config['K'] * (2**(len(self.table.buckets) - 1))
        return (num_contacts, num_nodes)
    
    def shutdown(self):
        """Closes the port and cancels pending later calls."""
        self.listenport.stopListening()
        try:
            self.next_checkpoint.cancel()
        except:
            pass
        self.expirer.shutdown()
        self.store.close()

    def krpc_ping(self, id, _krpc_sender):
        sender = {'id' : id}
        sender['host'] = _krpc_sender[0]
        sender['port'] = _krpc_sender[1]        
        n = self.Node().initWithDict(sender)
        n.conn = self.udp.connectionForAddr((n.host, n.port))
        self.insertNode(n, contacted=0)
        return {"id" : self.node.id}
        
    def krpc_find_node(self, target, id, _krpc_sender):
        nodes = self.table.findNodes(target)
        nodes = map(lambda node: node.senderDict(), nodes)
        sender = {'id' : id}
        sender['host'] = _krpc_sender[0]
        sender['port'] = _krpc_sender[1]        
        n = self.Node().initWithDict(sender)
        n.conn = self.udp.connectionForAddr((n.host, n.port))
        self.insertNode(n, contacted=0)
        return {"nodes" : nodes, "id" : self.node.id}


## This class provides read-only access to the DHT, valueForKey
## you probably want to use this mixin and provide your own write methods
class KhashmirRead(KhashmirBase):
    _Node = KNodeRead
    def retrieveValues(self, key):
        c = self.store.cursor()
        c.execute("select value from kv where key = %s;", sqlite.encode(key))
        t = c.fetchone()
        l = []
        while t:
            l.append(t['value'])
            t = c.fetchone()
        return l
    ## also async
    def valueForKey(self, key, callback, searchlocal = 1):
        """ returns the values found for key in global table
            callback will be called with a list of values for each peer that returns unique values
            final callback will be an empty list - probably should change to 'more coming' arg
        """
        nodes = self.table.findNodes(key)
        
        # get locals
        if searchlocal:
            l = self.retrieveValues(key)
            if len(l) > 0:
                reactor.callLater(0, callback, (l))
        else:
            l = []
        
        # create our search state
        state = GetValue(self, key, callback, self.config)
        reactor.callLater(0, state.goWithNodes, nodes, l)

    def krpc_find_value(self, key, id, _krpc_sender):
        sender = {'id' : id}
        sender['host'] = _krpc_sender[0]
        sender['port'] = _krpc_sender[1]        
        n = self.Node().initWithDict(sender)
        n.conn = self.udp.connectionForAddr((n.host, n.port))
        self.insertNode(n, contacted=0)
    
        l = self.retrieveValues(key)
        if len(l) > 0:
            return {'values' : l, "id": self.node.id}
        else:
            nodes = self.table.findNodes(key)
            nodes = map(lambda node: node.senderDict(), nodes)
            return {'nodes' : nodes, "id": self.node.id}

###  provides a generic write method, you probably don't want to deploy something that allows
###  arbitrary value storage
class KhashmirWrite(KhashmirRead):
    _Node = KNodeWrite
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
            action = StoreValue(self.table, key, value, response, self.config)
            reactor.callLater(0, action.goWithNodes, nodes)
            
        # this call is asynch
        self.findNode(key, _storeValueForKey)
                    
    def krpc_store_value(self, key, value, id, _krpc_sender):
        t = "%0.6f" % time()
        c = self.store.cursor()
        try:
            c.execute("insert into kv values (%s, %s, %s);", (sqlite.encode(key), sqlite.encode(value), t))
        except sqlite.IntegrityError, reason:
            # update last insert time
            c.execute("update kv set time = %s where key = %s and value = %s;", (t, sqlite.encode(key), sqlite.encode(value)))
        self.store.commit()
        sender = {'id' : id}
        sender['host'] = _krpc_sender[0]
        sender['port'] = _krpc_sender[1]        
        n = self.Node().initWithDict(sender)
        n.conn = self.udp.connectionForAddr((n.host, n.port))
        self.insertNode(n, contacted=0)
        return {"id" : self.node.id}

# the whole shebang, for testing
class Khashmir(KhashmirWrite):
    _Node = KNodeWrite

class SimpleTests(unittest.TestCase):
    
    timeout = 10
    DHT_DEFAULTS = {'PORT': 9977, 'K': 8, 'HASH_LENGTH': 160,
                    'CHECKPOINT_INTERVAL': 900, 'CONCURRENT_REQS': 4,
                    'STORE_REDUNDANCY': 3, 'MAX_FAILURES': 3,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KEINITIAL_DELAY': 15, 'KE_DELAY': 1200,
                    'KE_AGE': 3600, }

    def setUp(self):
        krpc.KRPC.noisy = 0
        d = self.DHT_DEFAULTS.copy()
        d['PORT'] = 4044
        self.a = Khashmir(d)
        d = self.DHT_DEFAULTS.copy()
        d['PORT'] = 4045
        self.b = Khashmir(d)
        
    def tearDown(self):
        self.a.shutdown()
        self.b.shutdown()
        os.unlink(self.a.db)
        os.unlink(self.b.db)

    def testAddContact(self):
        self.assertEqual(len(self.a.table.buckets), 1)
        self.assertEqual(len(self.a.table.buckets[0].l), 0)

        self.assertEqual(len(self.b.table.buckets), 1)
        self.assertEqual(len(self.b.table.buckets[0].l), 0)

        self.a.addContact('127.0.0.1', 4045)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()

        self.assertEqual(len(self.a.table.buckets), 1)
        self.assertEqual(len(self.a.table.buckets[0].l), 1)
        self.assertEqual(len(self.b.table.buckets), 1)
        self.assertEqual(len(self.b.table.buckets[0].l), 1)

    def testStoreRetrieve(self):
        self.a.addContact('127.0.0.1', 4045)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.got = 0
        self.a.storeValueForKey(sha('foo').digest(), 'foobar')
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.a.valueForKey(sha('foo').digest(), self._cb)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()

    def _cb(self, val):
        if not val:
            self.assertEqual(self.got, 1)
        elif 'foobar' in val:
            self.got = 1


class MultiTest(unittest.TestCase):
    
    timeout = 30
    num = 20
    DHT_DEFAULTS = {'PORT': 9977, 'K': 8, 'HASH_LENGTH': 160,
                    'CHECKPOINT_INTERVAL': 900, 'CONCURRENT_REQS': 4,
                    'STORE_REDUNDANCY': 3, 'MAX_FAILURES': 3,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KEINITIAL_DELAY': 15, 'KE_DELAY': 1200,
                    'KE_AGE': 3600, }

    def _done(self, val):
        self.done = 1
        
    def setUp(self):
        self.l = []
        self.startport = 4088
        for i in range(self.num):
            d = self.DHT_DEFAULTS.copy()
            d['PORT'] = self.startport + i
            self.l.append(Khashmir(d))
        reactor.iterate()
        reactor.iterate()
        
        for i in self.l:
            i.addContact('127.0.0.1', self.l[randrange(0,self.num)].port)
            i.addContact('127.0.0.1', self.l[randrange(0,self.num)].port)
            i.addContact('127.0.0.1', self.l[randrange(0,self.num)].port)
            reactor.iterate()
            reactor.iterate()
            reactor.iterate() 
            
        for i in self.l:
            self.done = 0
            i.findCloseNodes(self._done)
            while not self.done:
                reactor.iterate()
        for i in self.l:
            self.done = 0
            i.findCloseNodes(self._done)
            while not self.done:
                reactor.iterate()

    def tearDown(self):
        for i in self.l:
            i.shutdown()
            os.unlink(i.db)
            
        reactor.iterate()
        
    def testStoreRetrieve(self):
        for i in range(10):
            K = newID()
            V = newID()
            
            for a in range(3):
                self.done = 0
                def _scb(val):
                    self.done = 1
                self.l[randrange(0, self.num)].storeValueForKey(K, V, _scb)
                while not self.done:
                    reactor.iterate()


                def _rcb(val):
                    if not val:
                        self.done = 1
                        self.assertEqual(self.got, 1)
                    elif V in val:
                        self.got = 1
                for x in range(3):
                    self.got = 0
                    self.done = 0
                    self.l[randrange(0, self.num)].valueForKey(K, _rcb)
                    while not self.done:
                        reactor.iterate()
