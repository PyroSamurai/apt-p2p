## Copyright 2002-2004 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

"""The main Khashmir program."""

import warnings
warnings.simplefilter("ignore", DeprecationWarning)

from datetime import datetime, timedelta
from random import randrange, shuffle
from sha import sha
import os

from twisted.internet.defer import Deferred
from twisted.internet import protocol, reactor
from twisted.python import log
from twisted.trial import unittest

from db import DB
from ktable import KTable
from knode import KNodeBase, KNodeRead, KNodeWrite, NULL_ID
from khash import newID, newIDInRange
from actions import FindNode, FindValue, GetValue, StoreValue
from stats import StatsLogger
import krpc

class KhashmirBase(protocol.Factory):
    """The base Khashmir class, with base functionality and find node, no key-value mappings.
    
    @type _Node: L{node.Node}
    @ivar _Node: the knode implementation to use for this class of DHT
    @type config: C{dictionary}
    @ivar config: the configuration parameters for the DHT
    @type port: C{int}
    @ivar port: the port to listen on
    @type store: L{db.DB}
    @ivar store: the database to store nodes and key/value pairs in
    @type node: L{node.Node}
    @ivar node: this node
    @type table: L{ktable.KTable}
    @ivar table: the routing table
    @type token_secrets: C{list} of C{string}
    @ivar token_secrets: the current secrets to use to create tokens
    @type stats: L{stats.StatsLogger}
    @ivar stats: the statistics gatherer
    @type udp: L{krpc.hostbroker}
    @ivar udp: the factory for the KRPC protocol
    @type listenport: L{twisted.internet.interfaces.IListeningPort}
    @ivar listenport: the UDP listening port
    @type next_checkpoint: L{twisted.internet.interfaces.IDelayedCall}
    @ivar next_checkpoint: the delayed call for the next checkpoint
    """
    
    _Node = KNodeBase
    
    def __init__(self, config, cache_dir='/tmp'):
        """Initialize the Khashmir class and call the L{setup} method.
        
        @type config: C{dictionary}
        @param config: the configuration parameters for the DHT
        @type cache_dir: C{string}
        @param cache_dir: the directory to store all files in
            (optional, defaults to the /tmp directory)
        """
        self.config = None
        self.setup(config, cache_dir)
        
    def setup(self, config, cache_dir):
        """Setup all the Khashmir sub-modules.
        
        @type config: C{dictionary}
        @param config: the configuration parameters for the DHT
        @type cache_dir: C{string}
        @param cache_dir: the directory to store all files in
        """
        self.config = config
        self.port = config['PORT']
        self.store = DB(os.path.join(cache_dir, 'khashmir.' + str(self.port) + '.db'))
        self.node = self._loadSelfNode('', self.port)
        self.table = KTable(self.node, config)
        self.token_secrets = [newID()]
        self.stats = StatsLogger(self.table, self.store, self.config)
        
        # Start listening
        self.udp = krpc.hostbroker(self, self.stats, config)
        self.udp.protocol = krpc.KRPC
        self.listenport = reactor.listenUDP(self.port, self.udp)
        
        # Load the routing table and begin checkpointing
        self._loadRoutingTable()
        self.refreshTable(force = True)
        self.next_checkpoint = reactor.callLater(60, self.checkpoint)

    def Node(self, id, host = None, port = None):
        """Create a new node.
        
        @see: L{node.Node.__init__}
        """
        n = self._Node(id, host, port)
        n.table = self.table
        n.conn = self.udp.connectionForAddr((n.host, n.port))
        return n
    
    def __del__(self):
        """Stop listening for packets."""
        self.listenport.stopListening()
        
    def _loadSelfNode(self, host, port):
        """Create this node, loading any previously saved one."""
        id = self.store.getSelfNode()
        if not id:
            id = newID()
        return self._Node(id, host, port)
        
    def checkpoint(self):
        """Perform some periodic maintenance operations."""
        # Create a new token secret
        self.token_secrets.insert(0, newID())
        if len(self.token_secrets) > 3:
            self.token_secrets.pop()
            
        # Save some parameters for reloading
        self.store.saveSelfNode(self.node.id)
        self.store.dumpRoutingTable(self.table.buckets)
        
        # DHT maintenance
        self.store.expireValues(self.config['KEY_EXPIRE'])
        self.refreshTable()
        
        self.next_checkpoint = reactor.callLater(randrange(int(self.config['CHECKPOINT_INTERVAL'] * .9), 
                                                           int(self.config['CHECKPOINT_INTERVAL'] * 1.1)), 
                                                 self.checkpoint)
        
    def _loadRoutingTable(self):
        """Load the previous routing table nodes from the database.
        
        It's usually a good idea to call refreshTable(force = True) after
        loading the table.
        """
        nodes = self.store.getRoutingTable()
        for rec in nodes:
            n = self.Node(rec[0], rec[1], int(rec[2]))
            self.table.insertNode(n, contacted = False)
            
    #{ Local interface
    def addContact(self, host, port, callback=None, errback=None):
        """Ping this node and add the contact info to the table on pong.
        
        @type host: C{string}
        @param host: the IP address of the node to contact
        @type port: C{int}
        @param port:the port of the node to contact
        @type callback: C{method}
        @param callback: the method to call with the results, it must take 1
            parameter, the contact info returned by the node
            (optional, defaults to doing nothing with the results)
        @type errback: C{method}
        @param errback: the method to call if an error occurs
            (optional, defaults to calling the callback with None)
        """
        n = self.Node(NULL_ID, host, port)
        self.sendJoin(n, callback=callback, errback=errback)

    def findNode(self, id, callback, errback=None):
        """Find the contact info for the K closest nodes in the global table.
        
        @type id: C{string}
        @param id: the target ID to find the K closest nodes of
        @type callback: C{method}
        @param callback: the method to call with the results, it must take 1
            parameter, the list of K closest nodes
        @type errback: C{method}
        @param errback: the method to call if an error occurs
            (optional, defaults to doing nothing when an error occurs)
        """
        # Get K nodes out of local table/cache
        nodes = self.table.findNodes(id)
        d = Deferred()
        if errback:
            d.addCallbacks(callback, errback)
        else:
            d.addCallback(callback)

        # If the target ID was found
        if len(nodes) == 1 and nodes[0].id == id:
            d.callback(nodes)
        else:
            # Start the finding nodes action
            state = FindNode(self, id, d.callback, self.config, self.stats)
            reactor.callLater(0, state.goWithNodes, nodes)
    
    def insertNode(self, node, contacted = True):
        """Try to insert a node in our local table, pinging oldest contact if necessary.
        
        If all you have is a host/port, then use L{addContact}, which calls this
        method after receiving the PONG from the remote node. The reason for
        the seperation is we can't insert a node into the table without its
        node ID. That means of course the node passed into this method needs
        to be a properly formed Node object with a valid ID.

        @type node: L{node.Node}
        @param node: the new node to try and insert
        @type contacted: C{boolean}
        @param contacted: whether the new node is known to be good, i.e.
            responded to a request (optional, defaults to True)
        """
        old = self.table.insertNode(node, contacted=contacted)
        if (old and old.id != self.node.id and
            (datetime.now() - old.lastSeen) > 
             timedelta(seconds=self.config['MIN_PING_INTERVAL'])):
            
            def _staleNodeHandler(err, oldnode = old, newnode = node, self = self):
                """The pinged node never responded, so replace it."""
                log.msg("ping failed (%s) %s/%s" % (self.config['PORT'], oldnode.host, oldnode.port))
                log.err(err)
                self.table.replaceStaleNode(oldnode, newnode)
            
            def _notStaleNodeHandler(dict, old=old, self=self):
                """Got a pong from the old node, so update it."""
                dict = dict['rsp']
                if dict['id'] == old.id:
                    self.table.justSeenNode(old.id)
            
            # Bucket is full, check to see if old node is still available
            self.stats.startedAction('ping')
            df = old.ping(self.node.id)
            df.addCallbacks(_notStaleNodeHandler, _staleNodeHandler)

    def sendJoin(self, node, callback=None, errback=None):
        """Join the DHT by pinging a bootstrap node.
        
        @type node: L{node.Node}
        @param node: the node to send the join to
        @type callback: C{method}
        @param callback: the method to call with the results, it must take 1
            parameter, the contact info returned by the node
            (optional, defaults to doing nothing with the results)
        @type errback: C{method}
        @param errback: the method to call if an error occurs
            (optional, defaults to calling the callback with None)
        """

        def _pongHandler(dict, node=node, self=self, callback=callback):
            """Node responded properly, callback with response."""
            n = self.Node(dict['rsp']['id'], dict['_krpc_sender'][0], dict['_krpc_sender'][1])
            self.insertNode(n)
            if callback:
                callback((dict['rsp']['ip_addr'], dict['rsp']['port']))

        def _defaultPong(err, node=node, self=self, callback=callback, errback=errback):
            """Error occurred, fail node and errback or callback with error."""
            log.msg("join failed (%s) %s/%s" % (self.config['PORT'], node.host, node.port))
            log.err(err)
            self.table.nodeFailed(node)
            if errback:
                errback()
            elif callback:
                callback(None)
        
        self.stats.startedAction('join')
        df = node.join(self.node.id)
        df.addCallbacks(_pongHandler, _defaultPong)

    def findCloseNodes(self, callback=lambda a: None, errback = None):
        """Perform a findNode on the ID one away from our own.

        This will allow us to populate our table with nodes on our network
        closest to our own. This is called as soon as we start up with an
        empty table.

        @type callback: C{method}
        @param callback: the method to call with the results, it must take 1
            parameter, the list of K closest nodes
            (optional, defaults to doing nothing with the results)
        @type errback: C{method}
        @param errback: the method to call if an error occurs
            (optional, defaults to doing nothing when an error occurs)
        """
        id = self.node.id[:-1] + chr((ord(self.node.id[-1]) + 1) % 256)
        self.findNode(id, callback, errback)

    def refreshTable(self, force = False):
        """Check all the buckets for those that need refreshing.
        
        @param force: refresh all buckets regardless of last bucket access time
            (optional, defaults to False)
        """
        def callback(nodes):
            pass
    
        for bucket in self.table.buckets:
            if force or (datetime.now() - bucket.lastAccessed > 
                         timedelta(seconds=self.config['BUCKET_STALENESS'])):
                # Choose a random ID in the bucket and try and find it
                id = newIDInRange(bucket.min, bucket.max)
                self.findNode(id, callback)

    def shutdown(self):
        """Closes the port and cancels pending later calls."""
        self.listenport.stopListening()
        try:
            self.next_checkpoint.cancel()
        except:
            pass
        self.store.close()
    
    def getStats(self):
        """Gather the statistics for the DHT."""
        return self.stats.formatHTML()

    #{ Remote interface
    def krpc_ping(self, id, _krpc_sender):
        """Pong with our ID.
        
        @type id: C{string}
        @param id: the node ID of the sender node
        @type _krpc_sender: (C{string}, C{int})
        @param _krpc_sender: the sender node's IP address and port
        """
        n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
        self.insertNode(n, contacted = False)

        return {"id" : self.node.id}
        
    def krpc_join(self, id, _krpc_sender):
        """Add the node by responding with its address and port.
        
        @type id: C{string}
        @param id: the node ID of the sender node
        @type _krpc_sender: (C{string}, C{int})
        @param _krpc_sender: the sender node's IP address and port
        """
        n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
        self.insertNode(n, contacted = False)

        return {"ip_addr" : _krpc_sender[0], "port" : _krpc_sender[1], "id" : self.node.id}
        
    def krpc_find_node(self, target, id, _krpc_sender):
        """Find the K closest nodes to the target in the local routing table.
        
        @type target: C{string}
        @param target: the target ID to find nodes for
        @type id: C{string}
        @param id: the node ID of the sender node
        @type _krpc_sender: (C{string}, C{int})
        @param _krpc_sender: the sender node's IP address and port
        """
        n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
        self.insertNode(n, contacted = False)

        nodes = self.table.findNodes(target)
        nodes = map(lambda node: node.contactInfo(), nodes)
        token = sha(self.token_secrets[0] + _krpc_sender[0]).digest()
        return {"nodes" : nodes, "token" : token, "id" : self.node.id}


class KhashmirRead(KhashmirBase):
    """The read-only Khashmir class, which can only retrieve (not store) key/value mappings."""

    _Node = KNodeRead

    #{ Local interface
    def findValue(self, key, callback, errback=None):
        """Get the nodes that have values for the key from the global table.
        
        @type key: C{string}
        @param key: the target key to find the values for
        @type callback: C{method}
        @param callback: the method to call with the results, it must take 1
            parameter, the list of nodes with values
        @type errback: C{method}
        @param errback: the method to call if an error occurs
            (optional, defaults to doing nothing when an error occurs)
        """
        # Get K nodes out of local table/cache
        nodes = self.table.findNodes(key)
        d = Deferred()
        if errback:
            d.addCallbacks(callback, errback)
        else:
            d.addCallback(callback)

        # Search for others starting with the locally found ones
        state = FindValue(self, key, d.callback, self.config, self.stats)
        reactor.callLater(0, state.goWithNodes, nodes)

    def valueForKey(self, key, callback, searchlocal = True):
        """Get the values found for key in global table.
        
        Callback will be called with a list of values for each peer that
        returns unique values. The final callback will be an empty list.

        @type key: C{string}
        @param key: the target key to get the values for
        @type callback: C{method}
        @param callback: the method to call with the results, it must take 2
            parameters: the key, and the values found
        @type searchlocal: C{boolean}
        @param searchlocal: whether to also look for any local values
        """
        # Get any local values
        if searchlocal:
            l = self.store.retrieveValues(key)
            if len(l) > 0:
                reactor.callLater(0, callback, key, l)
        else:
            l = []

        def _getValueForKey(nodes, key=key, local_values=l, response=callback, self=self):
            """Use the found nodes to send requests for values to."""
            state = GetValue(self, key, local_values, self.config['RETRIEVE_VALUES'], response, self.config, self.stats)
            reactor.callLater(0, state.goWithNodes, nodes)
            
        # First lookup nodes that have values for the key
        self.findValue(key, _getValueForKey)

    #{ Remote interface
    def krpc_find_value(self, key, id, _krpc_sender):
        """Find the number of values stored locally for the key, and the K closest nodes.
        
        @type key: C{string}
        @param key: the target key to find the values and nodes for
        @type id: C{string}
        @param id: the node ID of the sender node
        @type _krpc_sender: (C{string}, C{int})
        @param _krpc_sender: the sender node's IP address and port
        """
        n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
        self.insertNode(n, contacted = False)
    
        nodes = self.table.findNodes(key)
        nodes = map(lambda node: node.contactInfo(), nodes)
        num_values = self.store.countValues(key)
        return {'nodes' : nodes, 'num' : num_values, "id": self.node.id}

    def krpc_get_value(self, key, num, id, _krpc_sender):
        """Retrieve the values stored locally for the key.
        
        @type key: C{string}
        @param key: the target key to retrieve the values for
        @type num: C{int}
        @param num: the maximum number of values to retrieve, or 0 to
            retrieve all of them
        @type id: C{string}
        @param id: the node ID of the sender node
        @type _krpc_sender: (C{string}, C{int})
        @param _krpc_sender: the sender node's IP address and port
        """
        n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
        self.insertNode(n, contacted = False)
    
        l = self.store.retrieveValues(key)
        if num == 0 or num >= len(l):
            return {'values' : l, "id": self.node.id}
        else:
            shuffle(l)
            return {'values' : l[:num], "id": self.node.id}


class KhashmirWrite(KhashmirRead):
    """The read-write Khashmir class, which can store and retrieve key/value mappings."""

    _Node = KNodeWrite

    #{ Local interface
    def storeValueForKey(self, key, value, callback=None):
        """Stores the value for the key in the global table.
        
        No status in this implementation, peers respond but don't indicate
        status of storing values.

        @type key: C{string}
        @param key: the target key to store the value for
        @type value: C{string}
        @param value: the value to store with the key
        @type callback: C{method}
        @param callback: the method to call with the results, it must take 3
            parameters: the key, the value stored, and the result of the store
            (optional, defaults to doing nothing with the results)
        """
        def _storeValueForKey(nodes, key=key, value=value, response=callback, self=self):
            """Use the returned K closest nodes to store the key at."""
            if not response:
                def _storedValueHandler(key, value, sender):
                    """Default callback that does nothing."""
                    pass
                response = _storedValueHandler
            action = StoreValue(self, key, value, self.config['STORE_REDUNDANCY'], response, self.config, self.stats)
            reactor.callLater(0, action.goWithNodes, nodes)
            
        # First find the K closest nodes to operate on.
        self.findNode(key, _storeValueForKey)
                    
    #{ Remote interface
    def krpc_store_value(self, key, value, token, id, _krpc_sender):
        """Store the value locally with the key.
        
        @type key: C{string}
        @param key: the target key to store the value for
        @type value: C{string}
        @param value: the value to store with the key
        @param token: the token to confirm that this peer contacted us previously
        @type id: C{string}
        @param id: the node ID of the sender node
        @type _krpc_sender: (C{string}, C{int})
        @param _krpc_sender: the sender node's IP address and port
        """
        n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
        self.insertNode(n, contacted = False)
        for secret in self.token_secrets:
            this_token = sha(secret + _krpc_sender[0]).digest()
            if token == this_token:
                self.store.storeValue(key, value)
                return {"id" : self.node.id}
        raise krpc.KrpcError, (krpc.KRPC_ERROR_INVALID_TOKEN, 'token is invalid, do a find_nodes to get a fresh one')


class Khashmir(KhashmirWrite):
    """The default Khashmir class (currently the read-write L{KhashmirWrite})."""
    _Node = KNodeWrite


class SimpleTests(unittest.TestCase):
    
    timeout = 10
    DHT_DEFAULTS = {'PORT': 9977, 'K': 8, 'HASH_LENGTH': 160,
                    'CHECKPOINT_INTERVAL': 300, 'CONCURRENT_REQS': 4,
                    'STORE_REDUNDANCY': 3, 'RETRIEVE_VALUES': -10000,
                    'MAX_FAILURES': 3,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KEY_EXPIRE': 3600, 'SPEW': False, }

    def setUp(self):
        d = self.DHT_DEFAULTS.copy()
        d['PORT'] = 4044
        self.a = Khashmir(d)
        d = self.DHT_DEFAULTS.copy()
        d['PORT'] = 4045
        self.b = Khashmir(d)
        
    def tearDown(self):
        self.a.shutdown()
        self.b.shutdown()
        os.unlink(self.a.store.db)
        os.unlink(self.b.store.db)

    def testAddContact(self):
        self.failUnlessEqual(len(self.a.table.buckets), 1)
        self.failUnlessEqual(len(self.a.table.buckets[0].l), 0)

        self.failUnlessEqual(len(self.b.table.buckets), 1)
        self.failUnlessEqual(len(self.b.table.buckets[0].l), 0)

        self.a.addContact('127.0.0.1', 4045)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()

        self.failUnlessEqual(len(self.a.table.buckets), 1)
        self.failUnlessEqual(len(self.a.table.buckets[0].l), 1)
        self.failUnlessEqual(len(self.b.table.buckets), 1)
        self.failUnlessEqual(len(self.b.table.buckets[0].l), 1)

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

    def _cb(self, key, val):
        if not val:
            self.failUnlessEqual(self.got, 1)
        elif 'foobar' in val:
            self.got = 1


class MultiTest(unittest.TestCase):
    
    timeout = 30
    num = 20
    DHT_DEFAULTS = {'PORT': 9977, 'K': 8, 'HASH_LENGTH': 160,
                    'CHECKPOINT_INTERVAL': 300, 'CONCURRENT_REQS': 4,
                    'STORE_REDUNDANCY': 3, 'RETRIEVE_VALUES': -10000,
                    'MAX_FAILURES': 3,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KEY_EXPIRE': 3600, 'SPEW': False, }

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
            os.unlink(i.store.db)
            
        reactor.iterate()
        
    def testStoreRetrieve(self):
        for i in range(10):
            K = newID()
            V = newID()
            
            for a in range(3):
                self.done = 0
                def _scb(key, value, result):
                    self.done = 1
                self.l[randrange(0, self.num)].storeValueForKey(K, V, _scb)
                while not self.done:
                    reactor.iterate()


                def _rcb(key, val):
                    if not val:
                        self.done = 1
                        self.failUnlessEqual(self.got, 1)
                    elif V in val:
                        self.got = 1
                for x in range(3):
                    self.got = 0
                    self.done = 0
                    self.l[randrange(0, self.num)].valueForKey(K, _rcb)
                    while not self.done:
                        reactor.iterate()
