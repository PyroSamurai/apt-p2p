## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

from unittest import *
from krpc import *
#from airhook import *

KRPC.noisy = 0

import sys

if __name__ =="__main__":
    tests = defaultTestLoader.loadTestsFromNames([sys.argv[0][:-3]])
    result = TextTestRunner().run(tests)


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
    a = hostbroker(af)
    a.protocol = KRPC
    p = reactor.listenUDP(port, a)
    return af, a, p
    
class KRPCTests(TestCase):
    def setUp(self):
        self.noisy = 0
        self.af, self.a, self.ap = make(1180)
        self.bf, self.b, self.bp = make(1181)

    def tearDown(self):
        self.ap.stopListening()
        self.bp.stopListening()
        reactor.iterate()
        reactor.iterate()

    def testSimpleMessage(self):
        self.noisy = 0
        self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('store', {'msg' : "This is a test."})
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.bf.buf, ["This is a test."])

    def testMessageBlast(self):
        self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('store', {'msg' : "This is a test."})
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.bf.buf, ["This is a test."])
        self.bf.buf = []
        
        for i in range(100):
            self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('store', {'msg' : "This is a test."})
            reactor.iterate()
            #self.bf.buf = []
        self.assertEqual(self.bf.buf, ["This is a test."] * 100)

    def testEcho(self):
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is a test."})
        df.addCallback(self.gotMsg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.msg, "This is a test.")

    def gotMsg(self, dict):
        _krpc_sender = dict['_krpc_sender']
        msg = dict['rsp']
        self.msg = msg

    def testManyEcho(self):
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is a test."})
        df.addCallback(self.gotMsg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.msg, "This is a test.")
        for i in xrange(100):
            self.msg = None
            df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is a test."})
            df.addCallback(self.gotMsg)
            reactor.iterate()
            reactor.iterate()
            reactor.iterate()
            reactor.iterate()
            self.assertEqual(self.msg, "This is a test.")

    def testMultiEcho(self):
        self.noisy = 1
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is a test."})
        df.addCallback(self.gotMsg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.msg, "This is a test.")

        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is another test."})
        df.addCallback(self.gotMsg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.msg, "This is another test.")

        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is yet another test."})
        df.addCallback(self.gotMsg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.msg, "This is yet another test.")

    def testEchoReset(self):
        self.noisy = 1
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is a test."})
        df.addCallback(self.gotMsg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.msg, "This is a test.")

        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is another test."})
        df.addCallback(self.gotMsg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.msg, "This is another test.")

        del(self.a.connections[('127.0.0.1', 1181)])
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('echo', {'msg' : "This is yet another test."})
        df.addCallback(self.gotMsg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.msg, "This is yet another test.")

    def testLotsofEchoReset(self):
        for i in range(100):
            self.testEchoReset()
            
    def testUnknownMeth(self):
        self.noisy = 1
        df = self.a.connectionForAddr(('127.0.0.1', 1181)).sendRequest('blahblah', {'msg' : "This is a test."})
        df.addErrback(self.gotErr)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.err, KRPC_ERROR_METHOD_UNKNOWN)

    def gotErr(self, err):
        self.err = err.value
        