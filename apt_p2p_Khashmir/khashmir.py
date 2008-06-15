
"""The main Khashmir program.

@var isLocal: a compiled regular expression suitable for testing if an
    IP address is from a known local or private range
"""

import warnings
warnings.simplefilter("ignore", DeprecationWarning)

from datetime import datetime, timedelta
from random import randrange, shuffle
from sha import sha
from copy import copy
import os, re

from twisted.internet.defer import Deferred
from twisted.internet.base import DelayedCall
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

isLocal = re.compile('^(192\.168\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(172\.0?1[6-9]\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(172\.0?2[0-9]\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(172\.0?3[0-1]\.[0-9]{1,3}\.[0-9]{1,3})|'+
                     '(127\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})$')

class KhashmirBase(protocol.Factory):
    """The base Khashmir class, with base functionality and find node, no key-value mappings.
    
    @type _Node: L{node.Node}
    @ivar _Node: the knode implementation to use for this class of DHT
    @type config: C{dictionary}
    @ivar config: the configuration parameters for the DHT
    @type pinging: C{dictionary}
    @ivar pinging: the node's that are currently being pinged, keys are the
        node id's, values are the Deferred or DelayedCall objects
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
        self.pinging = {}
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
        self.stats = StatsLogger(self.table, self.store)
        
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
        if not id or not id.endswith(self.config['VERSION']):
            id = newID(self.config['VERSION'])
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
            (optional, defaults to calling the callback with the error)
        """
        n = self.Node(NULL_ID, host, port)
        self.sendJoin(n, callback=callback, errback=errback)

    def findNode(self, id, callback):
        """Find the contact info for the K closest nodes in the global table.
        
        @type id: C{string}
        @param id: the target ID to find the K closest nodes of
        @type callback: C{method}
        @param callback: the method to call with the results, it must take 1
            parameter, the list of K closest nodes
        """
        # Mark the bucket as having been accessed
        self.table.touch(id)
        
        # Start with our node
        nodes = [copy(self.node)]

        # Start the finding nodes action
        state = FindNode(self, id, callback, self.config, self.stats)
        reactor.callLater(0, state.goWithNodes, nodes)
    
    def insertNode(self, node, contacted = True):
        """Try to insert a node in our local table, pinging oldest contact if necessary.
        
        If all you have is a host/port, then use L{addContact}, which calls this
        method after receiving the PONG from the remote node. The reason for
        the separation is we can't insert a node into the table without its
        node ID. That means of course the node passed into this method needs
        to be a properly formed Node object with a valid ID.

        @type node: L{node.Node}
        @param node: the new node to try and insert
        @type contacted: C{boolean}
        @param contacted: whether the new node is known to be good, i.e.
            responded to a request (optional, defaults to True)
        """
        # Don't add any local nodes to the routing table
        if not self.config['LOCAL_OK'] and isLocal.match(node.host):
            log.msg('Not adding local node to table: %s/%s' % (node.host, node.port))
            return
        
        old = self.table.insertNode(node, contacted=contacted)

        if (isinstance(old, self._Node) and old.id != self.node.id and
            (datetime.now() - old.lastSeen) > 
             timedelta(seconds=self.config['MIN_PING_INTERVAL'])):
            
            # Bucket is full, check to see if old node is still available
            df = self.sendPing(old)
            df.addErrback(self._staleNodeHandler, old, node, contacted)
        elif not old and not contacted:
            # There's room, we just need to contact the node first
            df = self.sendPing(node)
            # Also schedule a future ping to make sure the node works
            def rePing(newnode, self = self):
                if newnode.id not in self.pinging:
                    self.pinging[newnode.id] = reactor.callLater(self.config['MIN_PING_INTERVAL'],
                                                                 self.sendPing, newnode)
                return newnode
            df.addCallback(rePing)

    def _staleNodeHandler(self, err, old, node, contacted):
        """The pinged node never responded, so replace it."""
        self.table.invalidateNode(old)
        self.insertNode(node, contacted)
        return err
    
    def nodeFailed(self, node):
        """Mark a node as having failed a request and schedule a future check.
        
        @type node: L{node.Node}
        @param node: the new node to try and insert
        """
        exists = self.table.nodeFailed(node)
        
        # If in the table, schedule a ping, if one isn't already sent/scheduled
        if exists and node.id not in self.pinging:
            self.pinging[node.id] = reactor.callLater(self.config['MIN_PING_INTERVAL'],
                                                      self.sendPing, node)
    
    def sendPing(self, node):
        """Ping the node to see if it's still alive.
        
        @type node: L{node.Node}
        @param node: the node to send the join to
        """
        # Check for a ping already underway
        if (isinstance(self.pinging.get(node.id, None), DelayedCall) and
            self.pinging[node.id].active()):
            self.pinging[node.id].cancel()
        elif isinstance(self.pinging.get(node.id, None), Deferred):
            return self.pinging[node.id]

        self.stats.startedAction('ping')
        df = node.ping(self.node.id)
        self.pinging[node.id] = df
        df.addCallbacks(self._pingHandler, self._pingError,
                        callbackArgs = (node, datetime.now()),
                        errbackArgs = (node, datetime.now()))
        return df

    def _pingHandler(self, dict, node, start):
        """Node responded properly, update it and return the node object."""
        self.stats.completedAction('ping', start)
        del self.pinging[node.id]
        # Create the node using the returned contact info
        n = self.Node(dict['id'], dict['_krpc_sender'][0], dict['_krpc_sender'][1])
        reactor.callLater(0, self.insertNode, n)
        return n

    def _pingError(self, err, node, start):
        """Error occurred, fail node."""
        log.msg("action ping failed on %s/%s: %s" % (node.host, node.port, err.getErrorMessage()))
        self.stats.completedAction('ping', start)
        del self.pinging[node.id]
        self.nodeFailed(node)
        return err
        
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
            (optional, defaults to calling the callback with the error)
        """
        if errback is None:
            errback = callback
        self.stats.startedAction('join')
        df = node.join(self.node.id)
        df.addCallbacks(self._joinHandler, self._joinError,
                        callbackArgs = (node, datetime.now()),
                        errbackArgs = (node, datetime.now()))
        if callback:
            df.addCallbacks(callback, errback)

    def _joinHandler(self, dict, node, start):
        """Node responded properly, extract the response."""
        self.stats.completedAction('join', start)
        # Create the node using the returned contact info
        n = self.Node(dict['id'], dict['_krpc_sender'][0], dict['_krpc_sender'][1])
        reactor.callLater(0, self.insertNode, n)
        return (dict['ip_addr'], dict['port'])

    def _joinError(self, err, node, start):
        """Error occurred, fail node."""
        log.msg("action join failed on %s/%s: %s" % (node.host, node.port, err.getErrorMessage()))
        self.stats.completedAction('join', start)
        self.nodeFailed(node)
        return err
        
    def findCloseNodes(self, callback=lambda a: None):
        """Perform a findNode on the ID one away from our own.

        This will allow us to populate our table with nodes on our network
        closest to our own. This is called as soon as we start up with an
        empty table.

        @type callback: C{method}
        @param callback: the method to call with the results, it must take 1
            parameter, the list of K closest nodes
            (optional, defaults to doing nothing with the results)
        """
        id = self.node.id[:-1] + chr((ord(self.node.id[-1]) + 1) % 256)
        self.findNode(id, callback)

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
        for nodeid in self.pinging.keys():
            if isinstance(self.pinging[nodeid], DelayedCall) and self.pinging[nodeid].active():
                self.pinging[nodeid].cancel()
                del self.pinging[nodeid]
        self.store.close()
    
    def getStats(self):
        """Gather the statistics for the DHT."""
        return self.stats.formatHTML()

    #{ Remote interface
    def krpc_ping(self, id, _krpc_sender = None):
        """Pong with our ID.
        
        @type id: C{string}
        @param id: the node ID of the sender node
        @type _krpc_sender: (C{string}, C{int})
        @param _krpc_sender: the sender node's IP address and port
        """
        if _krpc_sender is not None:
            n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
            reactor.callLater(0, self.insertNode, n, False)

        return {"id" : self.node.id}
        
    def krpc_join(self, id, _krpc_sender = None):
        """Add the node by responding with its address and port.
        
        @type id: C{string}
        @param id: the node ID of the sender node
        @type _krpc_sender: (C{string}, C{int})
        @param _krpc_sender: the sender node's IP address and port
        """
        if _krpc_sender is not None:
            n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
            reactor.callLater(0, self.insertNode, n, False)
        else:
            _krpc_sender = ('127.0.0.1', self.port)

        return {"ip_addr" : _krpc_sender[0], "port" : _krpc_sender[1], "id" : self.node.id}
        
    def krpc_find_node(self, id, target, _krpc_sender = None):
        """Find the K closest nodes to the target in the local routing table.
        
        @type target: C{string}
        @param target: the target ID to find nodes for
        @type id: C{string}
        @param id: the node ID of the sender node
        @type _krpc_sender: (C{string}, C{int})
        @param _krpc_sender: the sender node's IP address and port
        """
        if _krpc_sender is not None:
            n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
            reactor.callLater(0, self.insertNode, n, False)
        else:
            _krpc_sender = ('127.0.0.1', self.port)

        nodes = self.table.findNodes(target)
        nodes = map(lambda node: node.contactInfo(), nodes)
        token = sha(self.token_secrets[0] + _krpc_sender[0]).digest()
        return {"nodes" : nodes, "token" : token, "id" : self.node.id}


class KhashmirRead(KhashmirBase):
    """The read-only Khashmir class, which can only retrieve (not store) key/value mappings."""

    _Node = KNodeRead

    #{ Local interface
    def findValue(self, key, callback):
        """Get the nodes that have values for the key from the global table.
        
        @type key: C{string}
        @param key: the target key to find the values for
        @type callback: C{method}
        @param callback: the method to call with the results, it must take 1
            parameter, the list of nodes with values
        """
        # Mark the bucket as having been accessed
        self.table.touch(key)
        
        # Start with ourself
        nodes = [copy(self.node)]
        
        # Search for others starting with the locally found ones
        state = FindValue(self, key, callback, self.config, self.stats)
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

        def _getValueForKey(nodes, key=key, response=callback, self=self, searchlocal=searchlocal):
            """Use the found nodes to send requests for values to."""
            # Get any local values
            if searchlocal:
                l = self.store.retrieveValues(key)
                if len(l) > 0:
                    node = copy(self.node)
                    node.updateNumValues(len(l))
                    nodes = nodes + [node]

            state = GetValue(self, key, self.config['RETRIEVE_VALUES'], response, self.config, self.stats)
            reactor.callLater(0, state.goWithNodes, nodes)
            
        # First lookup nodes that have values for the key
        self.findValue(key, _getValueForKey)

    #{ Remote interface
    def krpc_find_value(self, id, key, _krpc_sender = None):
        """Find the number of values stored locally for the key, and the K closest nodes.
        
        @type key: C{string}
        @param key: the target key to find the values and nodes for
        @type id: C{string}
        @param id: the node ID of the sender node
        @type _krpc_sender: (C{string}, C{int})
        @param _krpc_sender: the sender node's IP address and port
        """
        if _krpc_sender is not None:
            n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
            reactor.callLater(0, self.insertNode, n, False)
    
        nodes = self.table.findNodes(key)
        nodes = map(lambda node: node.contactInfo(), nodes)
        num_values = self.store.countValues(key)
        return {'nodes' : nodes, 'num' : num_values, "id": self.node.id}

    def krpc_get_value(self, id, key, num, _krpc_sender = None):
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
        if _krpc_sender is not None:
            n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
            reactor.callLater(0, self.insertNode, n, False)
    
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
    def krpc_store_value(self, id, key, value, token, _krpc_sender = None):
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
        if _krpc_sender is not None:
            n = self.Node(id, _krpc_sender[0], _krpc_sender[1])
            reactor.callLater(0, self.insertNode, n, False)
        else:
            _krpc_sender = ('127.0.0.1', self.port)

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
    DHT_DEFAULTS = {'VERSION': 'A000', 'PORT': 9977,
                    'CHECKPOINT_INTERVAL': 300, 'CONCURRENT_REQS': 8,
                    'STORE_REDUNDANCY': 6, 'RETRIEVE_VALUES': -10000,
                    'MAX_FAILURES': 3, 'LOCAL_OK': True,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KRPC_TIMEOUT': 9, 'KRPC_INITIAL_DELAY': 2,
                    'KEY_EXPIRE': 3600, 'SPEW': True, }

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
        self.failUnlessEqual(len(self.a.table.buckets[0].nodes), 0)

        self.failUnlessEqual(len(self.b.table.buckets), 1)
        self.failUnlessEqual(len(self.b.table.buckets[0].nodes), 0)

        self.a.addContact('127.0.0.1', 4045)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()

        self.failUnlessEqual(len(self.a.table.buckets), 1)
        self.failUnlessEqual(len(self.a.table.buckets[0].nodes), 1)
        self.failUnlessEqual(len(self.b.table.buckets), 1)
        self.failUnlessEqual(len(self.b.table.buckets[0].nodes), 1)

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
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
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
    DHT_DEFAULTS = {'VERSION': 'A000', 'PORT': 9977,
                    'CHECKPOINT_INTERVAL': 300, 'CONCURRENT_REQS': 8,
                    'STORE_REDUNDANCY': 6, 'RETRIEVE_VALUES': -10000,
                    'MAX_FAILURES': 3, 'LOCAL_OK': True,
                    'MIN_PING_INTERVAL': 900,'BUCKET_STALENESS': 3600,
                    'KRPC_TIMEOUT': 9, 'KRPC_INITIAL_DELAY': 2,
                    'KEY_EXPIRE': 3600, 'SPEW': True, }

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
