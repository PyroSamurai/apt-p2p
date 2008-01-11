## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

from bencode import bencode, bdecode
from time import asctime
import sys
from traceback import format_exception

from twisted.internet.defer import Deferred
from twisted.internet import protocol, reactor
from twisted.trial import unittest

from khash import newID

KRPC_TIMEOUT = 20

KRPC_ERROR = 1
KRPC_ERROR_METHOD_UNKNOWN = 2
KRPC_ERROR_RECEIVED_UNKNOWN = 3
KRPC_ERROR_TIMEOUT = 4

# commands
TID = 't'
REQ = 'q'
RSP = 'r'
TYP = 'y'
ARG = 'a'
ERR = 'e'

class ProtocolError(Exception):
    pass

class hostbroker(protocol.DatagramProtocol):       
    def __init__(self, server, config):
        self.server = server
        self.config = config
        # this should be changed to storage that drops old entries
        self.connections = {}
        
    def datagramReceived(self, datagram, addr):
        #print `addr`, `datagram`
        #if addr != self.addr:
        c = self.connectionForAddr(addr)
        c.datagramReceived(datagram, addr)
        #if c.idle():
        #    del self.connections[addr]

    def connectionForAddr(self, addr):
        if addr == self.addr:
            raise Exception
        if not self.connections.has_key(addr):
            conn = self.protocol(addr, self.server, self.transport, self.config['SPEW'])
            self.connections[addr] = conn
        else:
            conn = self.connections[addr]
        return conn

    def makeConnection(self, transport):
        protocol.DatagramProtocol.makeConnection(self, transport)
        tup = transport.getHost()
        self.addr = (tup.host, tup.port)
        
    def stopProtocol(self):
        for conn in self.connections.values():
            conn.stop()
        protocol.DatagramProtocol.stopProtocol(self)

## connection
class KRPC:
    def __init__(self, addr, server, transport, spew = False):
        self.transport = transport
        self.factory = server
        self.addr = addr
        self.noisy = spew
        self.tids = {}
        self.stopped = False

    def datagramReceived(self, str, addr):
        if self.stopped:
            if self.noisy:
                print "stopped, dropping message from", addr, str
        # bdecode
        try:
            msg = bdecode(str)
        except Exception, e:
            if self.noisy:
                print "response decode error: " + `e`
        else:
            if self.noisy:
                print self.factory.port, "received from", addr, self.addr, ":", msg
            # look at msg type
            if msg[TYP]  == REQ:
                ilen = len(str)
                # if request
                #	tell factory to handle
                f = getattr(self.factory ,"krpc_" + msg[REQ], None)
                msg[ARG]['_krpc_sender'] =  self.addr
                if f and callable(f):
                    try:
                        ret = f(*(), **msg[ARG])
                    except Exception, e:
                        olen = self._sendResponse(addr, msg[TID], ERR, `format_exception(type(e), e, sys.exc_info()[2])`)
                    else:
                        olen = self._sendResponse(addr, msg[TID], RSP, ret)
                else:
                    if self.noisy:
                        print "don't know about method %s" % msg[REQ]
                    # unknown method
                    olen = self._sendResponse(addr, msg[TID], ERR, KRPC_ERROR_METHOD_UNKNOWN)
                if self.noisy:
                    print "%s %s >>> %s - %s %s %s" % (asctime(), addr, self.factory.node.port, 
                                                    ilen, msg[REQ], olen)
            elif msg[TYP] == RSP:
                # if response
                # 	lookup tid
                if self.tids.has_key(msg[TID]):
                    df = self.tids[msg[TID]]
                    # 	callback
                    del(self.tids[msg[TID]])
                    df.callback({'rsp' : msg[RSP], '_krpc_sender': addr})
                else:
                    print 'timeout ' + `msg[RSP]['id']`
                    # no tid, this transaction timed out already...
            elif msg[TYP] == ERR:
                # if error
                # 	lookup tid
                if self.tids.has_key(msg[TID]):
                    df = self.tids[msg[TID]]
                    # 	callback
                    df.errback(msg[ERR])
                    del(self.tids[msg[TID]])
                else:
                    # day late and dollar short
                    pass
            else:
                print "unknown message type " + `msg`
                # unknown message type
                df = self.tids[msg[TID]]
                # 	callback
                df.errback(KRPC_ERROR_RECEIVED_UNKNOWN)
                del(self.tids[msg[TID]])
                
    def _sendResponse(self, addr, tid, msgType, response):
        if not response:
            response = {}
            
        msg = {TID : tid, TYP : msgType, msgType : response}

        if self.noisy:
            print self.factory.port, "responding to", addr, ":", msg

        out = bencode(msg)
        self.transport.write(out, addr)
        return len(out)
    
    def sendRequest(self, method, args):
        if self.stopped:
            raise ProtocolError, "connection has been stopped"
        # make message
        # send it
        msg = {TID : newID(), TYP : REQ,  REQ : method, ARG : args}
        if self.noisy:
            print self.factory.port, "sending to", self.addr, ":", msg
        str = bencode(msg)
        d = Deferred()
        self.tids[msg[TID]] = d
        def timeOut(tids = self.tids, id = msg[TID], msg = msg):
            if tids.has_key(id):
                df = tids[id]
                del(tids[id])
                print ">>>>>> KRPC_ERROR_TIMEOUT"
                df.errback(ProtocolError('timeout waiting for %r' % msg))
        later = reactor.callLater(KRPC_TIMEOUT, timeOut)
        def dropTimeOut(dict, later_call = later):
            if later_call.active():
                later_call.cancel()
            return dict
        d.addBoth(dropTimeOut)
        self.transport.write(str, self.addr)
        return d
    
    def stop(self):
        """Timeout all pending requests."""
        for df in self.tids.values():
            df.errback(ProtocolError('connection has been closed'))
        self.tids = {}
        self.stopped = True
 
def connectionForAddr(host, port):
    return host
    
class Receiver(protocol.Factory):
    protocol = KRPC
    def __init__(self):
        self.buf = []
    def krpc_store(self, msg, _krpc_sender):
        self.buf += [msg]
    def krpc_echo(self, msg, _krpc_sender):
        return msg

def make(port):
    af = Receiver()
    a = hostbroker(af, {'SPEW': False})
    a.protocol = KRPC
    p = reactor.listenUDP(port, a)
    return af, a, p
    
class KRPCTests(unittest.TestCase):
    def setUp(self):
        self.af, self.a, self.ap = make(1180)
        self.bf, self.b, self.bp = make(1181)

    def tearDown(self):
        self.ap.stopListening()
        self.bp.stopListening()

    def bufEquals(self, result, value):
        self.failUnlessEqual(self.bf.buf, value)

    def testSimpleMessage(self):
        d = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('store', {'msg' : "This is a test."})
        d.addCallback(self.bufEquals, ["This is a test."])
        return d

    def testMessageBlast(self):
        for i in range(100):
            d = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('store', {'msg' : "This is a test."})
        d.addCallback(self.bufEquals, ["This is a test."] * 100)
        return d

    def testEcho(self):
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is a test."})
        df.addCallback(self.gotMsg, "This is a test.")
        return df

    def gotMsg(self, dict, should_be):
        _krpc_sender = dict['_krpc_sender']
        msg = dict['rsp']
        self.failUnlessEqual(msg, should_be)

    def testManyEcho(self):
        for i in xrange(100):
            df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is a test."})
            df.addCallback(self.gotMsg, "This is a test.")
        return df

    def testMultiEcho(self):
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is a test."})
        df.addCallback(self.gotMsg, "This is a test.")

        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is another test."})
        df.addCallback(self.gotMsg, "This is another test.")

        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is yet another test."})
        df.addCallback(self.gotMsg, "This is yet another test.")
        
        return df

    def testEchoReset(self):
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is a test."})
        df.addCallback(self.gotMsg, "This is a test.")

        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is another test."})
        df.addCallback(self.gotMsg, "This is another test.")
        df.addCallback(self.echoReset)
        return df
    
    def echoReset(self, dict):
        del(self.a.connections[('127.0.0.1', 1181)])
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is yet another test."})
        df.addCallback(self.gotMsg, "This is yet another test.")
        return df

    def testUnknownMeth(self):
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('blahblah', {'msg' : "This is a test."})
        df.addErrback(self.gotErr, KRPC_ERROR_METHOD_UNKNOWN)
        return df

    def gotErr(self, err, should_be):
        self.failUnlessEqual(err.value, should_be)
