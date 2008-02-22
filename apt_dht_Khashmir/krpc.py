## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

from bencode import bencode, bdecode
from time import asctime

from twisted.internet.defer import Deferred
from twisted.internet import protocol, reactor
from twisted.python import log
from twisted.trial import unittest

from khash import newID

KRPC_TIMEOUT = 20

# Remote node errors
KRPC_ERROR = 200
KRPC_ERROR_SERVER_ERROR = 201
KRPC_ERROR_MALFORMED_PACKET = 202
KRPC_ERROR_METHOD_UNKNOWN = 203
KRPC_ERROR_MALFORMED_REQUEST = 204
KRPC_ERROR_INVALID_TOKEN = 205

# Local errors
KRPC_ERROR_INTERNAL = 100
KRPC_ERROR_RECEIVED_UNKNOWN = 101
KRPC_ERROR_TIMEOUT = 102
KRPC_ERROR_PROTOCOL_STOPPED = 103

# commands
TID = 't'
REQ = 'q'
RSP = 'r'
TYP = 'y'
ARG = 'a'
ERR = 'e'

class KrpcError(Exception):
    pass

def verifyMessage(msg):
    """Check received message for corruption and errors.
    
    @type msg: C{dictionary}
    @param msg: the dictionary of information received on the connection
    @raise KrpcError: if the message is corrupt
    """
    
    if type(msg) != dict:
        raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "not a dictionary")
    if TYP not in msg:
        raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "no message type")
    if msg[TYP] == REQ:
        if REQ not in msg:
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "request type not specified")
        if type(msg[REQ]) != str:
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "request type is not a string")
        if ARG not in msg:
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "no arguments for request")
        if type(msg[ARG]) != dict:
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "arguments for request are not in a dictionary")
    elif msg[TYP] == RSP:
        if RSP not in msg:
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "response not specified")
        if type(msg[RSP]) != dict:
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "response is not a dictionary")
    elif msg[TYP] == ERR:
        if ERR not in msg:
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "error not specified")
        if type(msg[ERR]) != list:
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "error is not a list")
        if len(msg[ERR]) != 2:
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "error is not a 2-element list")
        if type(msg[ERR][0]) not in (int, long):
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "error number is not a number")
        if type(msg[ERR][1]) != str:
            raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "error string is not a string")
#    else:
#        raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "unknown message type")
    if TID not in msg:
        raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "no transaction ID specified")
    if type(msg[TID]) != str:
        raise KrpcError, (KRPC_ERROR_MALFORMED_PACKET, "transaction id is not a string")

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

    def datagramReceived(self, data, addr):
        if self.stopped:
            if self.noisy:
                log.msg("stopped, dropping message from %r: %s" % (addr, data))
        # bdecode
        try:
            msg = bdecode(data)
        except Exception, e:
            if self.noisy:
                log.msg("krpc bdecode error: ")
                log.err(e)
            return

        try:
            verifyMessage(msg)
        except Exception, e:
            log.msg("krpc message verification error: ")
            log.err(e)
            return

        if self.noisy:
            log.msg("%d received from %r: %s" % (self.factory.port, addr, msg))
        # look at msg type
        if msg[TYP]  == REQ:
            ilen = len(data)
            # if request
            #	tell factory to handle
            f = getattr(self.factory ,"krpc_" + msg[REQ], None)
            msg[ARG]['_krpc_sender'] =  self.addr
            if f and callable(f):
                try:
                    ret = f(*(), **msg[ARG])
                except KrpcError, e:
                    olen = self._sendResponse(addr, msg[TID], ERR, [e[0], e[1]])
                except TypeError, e:
                    olen = self._sendResponse(addr, msg[TID], ERR,
                                              [KRPC_ERROR_MALFORMED_REQUEST, str(e)])
                except Exception, e:
                    olen = self._sendResponse(addr, msg[TID], ERR,
                                              [KRPC_ERROR_SERVER_ERROR, str(e)])
                else:
                    olen = self._sendResponse(addr, msg[TID], RSP, ret)
            else:
                if self.noisy:
                    log.msg("don't know about method %s" % msg[REQ])
                # unknown method
                olen = self._sendResponse(addr, msg[TID], ERR,
                                          [KRPC_ERROR_METHOD_UNKNOWN, "unknown method "+str(msg[REQ])])
            if self.noisy:
                log.msg("%s >>> %s - %s %s %s" % (addr, self.factory.node.port,
                                                  ilen, msg[REQ], olen))
        elif msg[TYP] == RSP:
            # if response
            # 	lookup tid
            if self.tids.has_key(msg[TID]):
                df = self.tids[msg[TID]]
                # 	callback
                del(self.tids[msg[TID]])
                df.callback({'rsp' : msg[RSP], '_krpc_sender': addr})
            else:
                # no tid, this transaction timed out already...
                if self.noisy:
                    log.msg('timeout: %r' % msg[RSP]['id'])
        elif msg[TYP] == ERR:
            # if error
            # 	lookup tid
            if self.tids.has_key(msg[TID]):
                df = self.tids[msg[TID]]
                del(self.tids[msg[TID]])
                # callback
                df.errback(KrpcError(*msg[ERR]))
            else:
                # day late and dollar short, just log it
                log.msg("Got an error for an unknown request: %r" % (msg[ERR], ))
                pass
        else:
            if self.noisy:
                log.msg("unknown message type: %r" % msg)
            # unknown message type
            if msg[TID] in self.tids:
                df = self.tids[msg[TID]]
                del(self.tids[msg[TID]])
                # callback
                df.errback(KrpcError(KRPC_ERROR_RECEIVED_UNKNOWN,
                                     "Received an unknown message type: %r" % msg[TYP]))
                
    def _sendResponse(self, addr, tid, msgType, response):
        if not response:
            response = {}
            
        msg = {TID : tid, TYP : msgType, msgType : response}

        if self.noisy:
            log.msg("%d responding to %r: %s" % (self.factory.port, addr, msg))

        out = bencode(msg)
        self.transport.write(out, addr)
        return len(out)
    
    def sendRequest(self, method, args):
        if self.stopped:
            raise KrpcError, (KRPC_ERROR_PROTOCOL_STOPPED, "cannot send, connection has been stopped")
        # make message
        # send it
        msg = {TID : newID(), TYP : REQ,  REQ : method, ARG : args}
        if self.noisy:
            log.msg("%d sending to %r: %s" % (self.factory.port, self.addr, msg))
        data = bencode(msg)
        d = Deferred()
        self.tids[msg[TID]] = d
        def timeOut(tids = self.tids, id = msg[TID], method = method, addr = self.addr):
            if tids.has_key(id):
                df = tids[id]
                del(tids[id])
                df.errback(KrpcError(KRPC_ERROR_TIMEOUT, "timeout waiting for '%s' from %r" % (method, addr)))
        later = reactor.callLater(KRPC_TIMEOUT, timeOut)
        def dropTimeOut(dict, later_call = later):
            if later_call.active():
                later_call.cancel()
            return dict
        d.addBoth(dropTimeOut)
        self.transport.write(data, self.addr)
        return d
    
    def stop(self):
        """Timeout all pending requests."""
        for df in self.tids.values():
            df.errback(KrpcError(KRPC_ERROR_PROTOCOL_STOPPED, 'connection has been stopped while waiting for response'))
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
        return {}
    def krpc_echo(self, msg, _krpc_sender):
        return {'msg': msg}

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
        self.failUnlessEqual(msg['msg'], should_be)

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
        df.addBoth(self.gotErr, KRPC_ERROR_METHOD_UNKNOWN)
        return df

    def testMalformedRequest(self):
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is a test.", 'foo': 'bar'})
        df.addBoth(self.gotErr, KRPC_ERROR_MALFORMED_REQUEST)
        return df

    def gotErr(self, err, should_be):
        self.failUnlessEqual(err.value[0], should_be)
