## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

import unittest
from airhook import *
from random import uniform as rand
from cStringIO import StringIO


if __name__ =="__main__":
    tests = unittest.defaultTestLoader.loadTestsFromNames(['test_airhook'])
    result = unittest.TextTestRunner().run(tests)

class Echo(protocol.Protocol):
    def dataReceived(self, data):
        self.transport.write(data)
        
class Noisy(protocol.Protocol):
    def dataReceived(self, data):
        print `data`

class Receiver(protocol.Protocol):
    def __init__(self):
        self.q = []
    def dataReceived(self, data):
        self.q.append(data)

class StreamReceiver(protocol.Protocol):
    def __init__(self):
        self.buf = ""
    def dataReceived(self, data):
        self.buf += data
                
def makeEcho(port):
    f = protocol.Factory(); f.protocol = Echo
    return listenAirhookStream(port, f)
def makeNoisy(port):
    f = protocol.Factory(); f.protocol = Noisy
    return listenAirhookStream(port, f)
def makeReceiver(port):
    f = protocol.Factory(); f.protocol = Receiver
    return listenAirhookStream(port, f)
def makeStreamReceiver(port):
    f = protocol.Factory(); f.protocol = StreamReceiver
    return listenAirhookStream(port, f)

class DummyTransport:
    def __init__(self):
        self.s = StringIO()
    def write(self, data, addr):
        self.s.write(data)
    def seek(self, num):
        return self.s.seek(num)
    def read(self):
        return self.s.read()
        
def test_createStartPacket():
    flags = 0 | FLAG_AIRHOOK | FLAG_SESSION 
    packet = chr(flags) + "\xff" + "\x00\x00" + pack("!L", long(rand(0, 2**32)))
    return packet

def test_createReply(session, observed, obseq, seq):
    flags = 0 | FLAG_AIRHOOK | FLAG_SESSION | FLAG_OBSERVED
    packet = chr(flags) + pack("!H", seq)[1] + pack("!H", obseq + 1) + pack("!L", session) + pack("!L", observed)
    return packet

def pscope(msg, noisy=0):
    # packet scope
    str = ""
    p = AirhookPacket(msg)
    str += "oseq: %s  seq: %s " %  (p.oseq, p.seq)
    if noisy:
        str += "packet: %s  \n" % (`p.datagram`)
    flags = p.flags
    str += "flags: "
    if flags & FLAG_SESSION:
        str += "FLAG_SESSION "
    if flags & FLAG_OBSERVED:
        str += "FLAG_OBSERVED "
    if flags & FLAG_MISSED:
        str += "FLAG_MISSED "
    if flags & FLAG_NEXT:
        str += "FLAG_NEXT "
    str += "\n"
    
    if p.observed != None:
        str += "OBSERVED: %s\n" % p.observed
    if p.session != None:
        str += "SESSION: %s\n" % p.session
    if p.next != None:
        str += "NEXT: %s\n" % p.next
    if p.missed:
        if noisy:
            str += "MISSED: " + `p.missed`
        else:
            str += "MISSED: " + `len(p.missed)`
        str += "\n"
    if p.msgs:
        if noisy:
            str += "MSGS: " + `p.msgs` + "\n"
        else:
            str += "MSGS: <%s> " % len(p.msgs)
        str += "\n"
    return str
            
# testing function
def swap(a, dir="", noisy=0):
    msg = ""
    while not msg:
        a.transport.seek(0)
        msg= a.transport.read()
        a.transport = DummyTransport()
        if not msg:
            a.sendNext()
    if noisy:
                print 6*dir + " " + pscope(msg)
    return msg
    
def runTillEmpty(a, b, prob=1.0, noisy=0):
    msga = ''
    msgb = ''
    while a.omsgq or b.omsgq or a.weMissed or b.weMissed or ord(msga[0]) & (FLAG_NEXT | FLAG_MISSED) or ord(msgb[0]) & (FLAG_NEXT | FLAG_MISSED):
        if rand(0,1) < prob:
            msga = swap(a, '>', noisy)
            b.datagramReceived(msga)
        else:
            msga = swap(a, '>', 0)
        if rand(0,1) < prob:
            msgb = swap(b, '<', noisy)
            a.datagramReceived(msgb)
        else:
            msgb = swap(b, '<', 0)

class UstrTests(unittest.TestCase):
    def u(self, seq):
        return ustr("%s%s" % (pack("!H", seq), 'foobar'))
        
    def testLT(self):
        self.failUnless(self.u(0) < self.u(1))
        self.failUnless(self.u(1) < self.u(2))
        self.failUnless(self.u(2**16 - 1) < self.u(0))
        self.failUnless(self.u(2**16 - 1) < self.u(1))
        
        self.failIf(self.u(1) < self.u(0))
        self.failIf(self.u(2) < self.u(1))
        self.failIf(self.u(0) < self.u(2**16 - 1))
        self.failIf(self.u(1) < self.u(2**16 - 1))
        
    def testLTE(self):
        self.failUnless(self.u(0) <= self.u(1))
        self.failUnless(self.u(1) <= self.u(2))
        self.failUnless(self.u(2) <= self.u(2))
        self.failUnless(self.u(2**16 - 1) <= self.u(0))
        self.failUnless(self.u(2**16 - 1) <= self.u(1))
        self.failUnless(self.u(2**16 - 1) <= self.u(2**16))

        self.failIf(self.u(1) <= self.u(0))
        self.failIf(self.u(2) <= self.u(1))
        self.failIf(self.u(0) <= self.u(2**16 - 1))
        self.failIf(self.u(1) <= self.u(2**16 - 1))
        
    def testGT(self):
        self.failUnless(self.u(1) > self.u(0))
        self.failUnless(self.u(2) > self.u(1))
        self.failUnless(self.u(0) > self.u(2**16 - 1))
        self.failUnless(self.u(1) > self.u(2**16 - 1))

        self.failIf(self.u(0) > self.u(1))
        self.failIf(self.u(1) > self.u(2))
        self.failIf(self.u(2**16 - 1) > self.u(0))
        self.failIf(self.u(2**16 - 1) > self.u(1))

    def testGTE(self):
        self.failUnless(self.u(1) >= self.u(0))
        self.failUnless(self.u(2) >= self.u(1))
        self.failUnless(self.u(2) >= self.u(2))
        self.failUnless(self.u(0) >= self.u(0))
        self.failUnless(self.u(1) >= self.u(1))
        self.failUnless(self.u(2**16 - 1) >= self.u(2**16 - 1))

        self.failIf(self.u(0) >= self.u(1))
        self.failIf(self.u(1) >= self.u(2))
        self.failIf(self.u(2**16 - 1) >= self.u(0))
        self.failIf(self.u(2**16 - 1) >= self.u(1))
        
    def testEQ(self):
        self.failUnless(self.u(0) == self.u(0))
        self.failUnless(self.u(1) == self.u(1))
        self.failUnless(self.u(2**16 - 1) == self.u(2**16-1))
    
        self.failIf(self.u(0) == self.u(1))
        self.failIf(self.u(1) == self.u(0))
        self.failIf(self.u(2**16 - 1) == self.u(0))

    def testNEQ(self):
        self.failUnless(self.u(1) != self.u(0))
        self.failUnless(self.u(2) != self.u(1))
        self.failIf(self.u(2) != self.u(2))
        self.failIf(self.u(0) != self.u(0))
        self.failIf(self.u(1) != self.u(1))
        self.failIf(self.u(2**16 - 1) != self.u(2**16 - 1))


class SimpleTest(unittest.TestCase):
    def setUp(self):
        self.noisy = 0
        self.a = AirhookConnection()
        self.a.makeConnection(DummyTransport())
        self.a.addr = ('127.0.0.1', 4444)
        self.b = AirhookConnection()
        self.b.makeConnection(DummyTransport())
        self.b.addr = ('127.0.0.1', 4444)

    def testReallySimple(self):
        # connect to eachother and send a few packets, observe sequence incrementing
        a = self.a
        b = self.b
        self.assertEqual(a.state, pending)
        self.assertEqual(b.state, pending)
        self.assertEqual(a.outSeq, 0)
        self.assertEqual(b.outSeq, 0)
        self.assertEqual(a.obSeq, 0)
        self.assertEqual(b.obSeq, 0)

        msg = swap(a, '>', self.noisy)		
        self.assertEqual(a.state, sent)
        self.assertEqual(a.outSeq, 1)
        self.assertEqual(a.obSeq, 0)

        b.datagramReceived(msg)
        self.assertEqual(b.inSeq, 0)
        self.assertEqual(b.obSeq, 0)
        msg = swap(b, '<', self.noisy)		
        self.assertEqual(b.state, sent)
        self.assertEqual(b.outSeq, 1)

        a.datagramReceived(msg)
        self.assertEqual(a.state, confirmed)
        self.assertEqual(a.obSeq, 0)
        self.assertEqual(a.inSeq, 0)
        msg = swap(a, '>', self.noisy)		
        self.assertEqual(a.outSeq, 2)

        b.datagramReceived(msg)
        self.assertEqual(b.state, confirmed)
        self.assertEqual(b.obSeq, 0)
        self.assertEqual(b.inSeq, 1)
        msg = swap(b, '<', self.noisy)		
        self.assertEqual(b.outSeq, 2)

        a.datagramReceived(msg)
        self.assertEqual(a.outSeq, 2)
        self.assertEqual(a.inSeq, 1)
        self.assertEqual(a.obSeq, 1)

class BasicTests(unittest.TestCase):
    def setUp(self):
        self.noisy = 0
        self.a = AirhookConnection()
        self.a.makeConnection(DummyTransport())
        self.a.addr = ('127.0.0.1', 4444)
        self.b = AirhookConnection()
        self.b.makeConnection(DummyTransport())
        self.b.addr = ('127.0.0.1', 4444)
        self.a.protocol = Receiver()
        self.b.protocol = Receiver()

    def testSimple(self):
        a = self.a
        b = self.b
        
        TESTMSG = "Howdy, Y'All!"
        a.omsgq.append(TESTMSG)
        a.sendNext()
        msg = swap(a, '>', self.noisy)
        
        b.datagramReceived(msg)
        msg = swap(b, '<', self.noisy)
        a.datagramReceived(msg)
        msg = swap(a, '>', self.noisy)
        b.datagramReceived(msg)
        
        self.assertEqual(b.inMsg, 1)
        self.assertEqual(len(b.protocol.q), 1)
        self.assertEqual(b.protocol.q[0], TESTMSG)
        
        msg = swap(b, '<', self.noisy)
        
        a.datagramReceived(msg)
        msg = swap(a, '>', self.noisy)
        b.datagramReceived(msg)
        
    def testLostFirst(self):
        a = self.a
        b = self.b
        
        TESTMSG = "Howdy, Y'All!"
        TESTMSG2 = "Yee Haw"
        
        a.omsgq.append(TESTMSG)
        msg = swap(a, '>', self.noisy)
        b.datagramReceived(msg)
        msg = swap(b, '<', self.noisy)
        self.assertEqual(b.state, sent)
        a.datagramReceived(msg)
        msg = swap(a, '>', self.noisy)

        del(msg) # dropping first message
        
        a.omsgq.append(TESTMSG2)
        msg = swap(a, '>', self.noisy)
    
        b.datagramReceived(msg)
        self.assertEqual(b.state, confirmed)
        self.assertEqual(len(b.protocol.q), 1)
        self.assertEqual(b.protocol.q[0], TESTMSG2)
        self.assertEqual(b.weMissed, [(1, 0)])
        msg = swap(b, '<', self.noisy)
        
        a.datagramReceived(msg)
                                
        msg = swap(a, '>', self.noisy)
        
        b.datagramReceived(msg)
        self.assertEqual(len(b.protocol.q), 2)
        b.protocol.q.sort()
        l = [TESTMSG2, TESTMSG]
        l.sort()
        self.assertEqual(b.protocol.q,l)
        
        msg = swap(b, '<', self.noisy)
        
        a.datagramReceived(msg)
        msg = swap(a, '>', self.noisy)
        b.datagramReceived(msg)
        
        msg = swap(b, '<', self.noisy)
        a.datagramReceived(msg)
        msg = swap(a, '>', self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, '<', self.noisy)
        a.datagramReceived(msg)
        msg = swap(a, '>', self.noisy)

        self.assertEqual(len(b.protocol.q), 2)
        b.protocol.q.sort()
        l = [TESTMSG2, TESTMSG]
        l.sort()
        self.assertEqual(b.protocol.q,l)

    def testLostSecond(self):
        a = self.a
        b = self.b
        
        TESTMSG = "Howdy, Y'All!"
        TESTMSG2 = "Yee Haw"
        
        a.omsgq.append(TESTMSG)
        msg = swap(a, '>', self.noisy)
        b.datagramReceived(msg)
        msg = swap(b, '<', self.noisy)
        self.assertEqual(b.state, sent)
        a.datagramReceived(msg)
        msg = swap(a, '>', self.noisy)

        a.omsgq.append(TESTMSG2)
        msg2 = swap(a, '>', self.noisy)
        del(msg2) # dropping second message

        assert(a.outMsgs[1] != None)

        b.datagramReceived(msg)
        self.assertEqual(b.state, confirmed)
        self.assertEqual(len(b.protocol.q), 1)
        self.assertEqual(b.protocol.q[0], TESTMSG)
        self.assertEqual(b.inMsg, 1)
        self.assertEqual(b.weMissed, [])
        msg = swap(b, '<', self.noisy)
        
        a.datagramReceived(msg)
        assert(a.outMsgs[1] != None)
        msg = swap(a, '>', self.noisy)

        b.datagramReceived(msg)
        self.assertEqual(b.state, confirmed)
        self.assertEqual(len(b.protocol.q), 1)
        self.assertEqual(b.protocol.q[0], TESTMSG)
        self.assertEqual(b.weMissed, [(2, 1)])
        msg = swap(b, '<', self.noisy)

        a.datagramReceived(msg)
        msg = swap(a, '>', self.noisy)

        b.datagramReceived(msg)
        self.assertEqual(len(b.protocol.q), 2)
        b.protocol.q.sort()
        l = [TESTMSG2, TESTMSG]
        l.sort()
        self.assertEqual(b.protocol.q,l)
        
        msg = swap(b, '<', self.noisy)

        a.datagramReceived(msg)
        msg = swap(a, '>', self.noisy)

        b.datagramReceived(msg)
        
        msg = swap(b, '<', self.noisy)

        a.datagramReceived(msg)
        msg = swap(a, '>', self.noisy)

        b.datagramReceived(msg)

        msg = swap(b, '<', self.noisy)

        a.datagramReceived(msg)


        msg = swap(a, '>', self.noisy)

        self.assertEqual(len(b.protocol.q), 2)
        b.protocol.q.sort()
        l = [TESTMSG2, TESTMSG]
        l.sort()
        self.assertEqual(b.protocol.q,l)

    def testDoubleDouble(self):
        a = self.a
        b = self.b
        
        TESTMSGA = "Howdy, Y'All!"
        TESTMSGB = "Yee Haw"
        TESTMSGC = "FOO BAR"
        TESTMSGD = "WING WANG"
        
        a.omsgq.append(TESTMSGA)
        a.omsgq.append(TESTMSGB)

        b.omsgq.append(TESTMSGC)
        b.omsgq.append(TESTMSGD)
        
        
        msg = swap(a, '>', self.noisy)
            

        b.datagramReceived(msg)
        
        msg = swap(b, '<', self.noisy)
        self.assertEqual(b.state, sent)
        a.datagramReceived(msg)

        msg = swap(a, '>', self.noisy)

        b.datagramReceived(msg)
        self.assertEqual(len(b.protocol.q), 2)
        l = [TESTMSGA, TESTMSGB]
        l.sort();b.protocol.q.sort()
        self.assertEqual(b.protocol.q, l)
        self.assertEqual(b.inMsg, 2)

        msg = swap(b, '<', self.noisy)
        a.datagramReceived(msg)
        
        self.assertEqual(len(a.protocol.q), 2)
        l = [TESTMSGC, TESTMSGD]
        l.sort();a.protocol.q.sort()
        self.assertEqual(a.protocol.q, l)
        self.assertEqual(a.inMsg, 2)

    def testDoubleDoubleProb(self, prob=0.25):
        a = self.a
        b = self.b

        TESTMSGA = "Howdy, Y'All!"
        TESTMSGB = "Yee Haw"
        TESTMSGC = "FOO BAR"
        TESTMSGD = "WING WANG"
        
        a.omsgq.append(TESTMSGA)
        a.omsgq.append(TESTMSGB)

        b.omsgq.append(TESTMSGC)
        b.omsgq.append(TESTMSGD)
        
        runTillEmpty(a, b, prob, self.noisy)
        
        self.assertEqual(a.state, confirmed)
        self.assertEqual(b.state, confirmed)
        self.assertEqual(len(b.protocol.q), 2)
        l = [TESTMSGA, TESTMSGB]
        l.sort();b.protocol.q.sort()
        self.assertEqual(b.protocol.q, l)
                
        self.assertEqual(len(a.protocol.q), 2)
        l = [TESTMSGC, TESTMSGD]
        l.sort();a.protocol.q.sort()
        self.assertEqual(a.protocol.q, l)

    def testOneWayBlast(self, num = 2**12):
        a = self.a
        b = self.b
        
        import sha
        
        
        for i in xrange(num):
            a.omsgq.append(sha.sha(`i`).digest())
        runTillEmpty(a, b, noisy=self.noisy)

        self.assertEqual(len(b.protocol.q), num)
        
    def testTwoWayBlast(self, num = 2**12, prob=0.5):
        a = self.a
        b = self.b

        import sha
        
        
        for i in xrange(num):
            a.omsgq.append(sha.sha('a' + `i`).digest())
            b.omsgq.append(sha.sha('b' + `i`).digest())
            
        runTillEmpty(a, b, prob, self.noisy)                    


        self.assertEqual(len(a.protocol.q), num)
        self.assertEqual(len(b.protocol.q), num)
        
    def testLimitMessageNumbers(self):
        a = self.a
        b = self.b
        import sha

        msg = swap(a, noisy=self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, noisy=self.noisy)
        a.datagramReceived(msg)
        
        
        for i in range(5000):
            a.omsgq.append(sha.sha('a' + 'i').digest())
        
        for i in range(5000 / 255):
            msg = swap(a, noisy=self.noisy)
            self.assertEqual(a.obSeq, 0)
        self.assertEqual(a.next, 255)
        self.assertEqual(a.outMsgNums[(a.outSeq-1) % 256], 254)

    def testConnectionReset(self):
        self.testTwoWayBlast()
        self.b.protocol.q = []
        a = self.a
        b = self.b
        msg = swap(a, noisy=self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, noisy=self.noisy)
        a.datagramReceived(msg)

        a.omsgq.append("TESTING")
        msg = swap(a, noisy=self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, noisy=self.noisy)
        a.datagramReceived(msg)

        self.assertEqual(b.protocol.q[0], "TESTING")
        self.assertEqual(b.state, confirmed)
        
        self.a = AirhookConnection()
        self.a.makeConnection(DummyTransport())
        self.a.addr = ('127.0.0.1', 4444)
        a = self.a
        
        a.omsgq.append("TESTING2")
        msg = swap(a, noisy=self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, noisy=self.noisy)
        a.datagramReceived(msg)
        
        self.assertEqual(len(b.protocol.q), 1)
        msg = swap(a, noisy=self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, noisy=self.noisy)
        a.datagramReceived(msg)

        self.assertEqual(len(b.protocol.q), 2)
        self.assertEqual(b.protocol.q[1], "TESTING2")

    def testRecipientReset(self):
        self.testTwoWayBlast()
        self.b.protocol.q = []
        self.noisy = 0
        a = self.a
        b = self.b
        msg = swap(a, noisy=self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, noisy=self.noisy)
        a.datagramReceived(msg)

        a.omsgq.append("TESTING")
        msg = swap(a, noisy=self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, noisy=self.noisy)
        a.datagramReceived(msg)

        self.assertEqual(b.protocol.q[0], "TESTING")
        self.assertEqual(b.state, confirmed)
        
        self.b = AirhookConnection()
        self.b.makeConnection(DummyTransport())
        self.b.protocol = Receiver()
        self.b.addr = ('127.0.0.1', 4444)
        b = self.b
        
        msg = swap(a, noisy=self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, noisy=self.noisy)
        a.datagramReceived(msg)
        
        a.omsgq.append("TESTING2")
        self.assertEqual(len(b.protocol.q), 0)
        msg = swap(a, noisy=self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, noisy=self.noisy)
        a.datagramReceived(msg)

        msg = swap(a, noisy=self.noisy)
        b.datagramReceived(msg)

        msg = swap(b, noisy=self.noisy)
        a.datagramReceived(msg)

        self.assertEqual(len(b.protocol.q), 1)
        self.assertEqual(b.protocol.q[0], "TESTING2")

        
class StreamTests(unittest.TestCase):
    def setUp(self):
        self.noisy = 0
        self.a = StreamConnection()
        self.a.makeConnection(DummyTransport())
        self.a.addr = ('127.0.0.1', 4444)
        self.b = StreamConnection()
        self.b.makeConnection(DummyTransport())
        self.b.addr = ('127.0.0.1', 4444)
        self.a.protocol = StreamReceiver()
        self.b.protocol = StreamReceiver()

    def testStreamSimple(self, num = 2**12, prob=1.0):
        f = open('/dev/urandom', 'r')
        a = self.a
        b = self.b

        MSGA = f.read(num)
        MSGB = f.read(num)
        self.a.write(MSGA)
        self.b.write(MSGB)
        
        runTillEmpty(a, b, prob, self.noisy)
                
        self.assertEqual(len(a.protocol.buf), len(MSGB))
        self.assertEqual(len(b.protocol.buf), len(MSGA))
        self.assertEqual(a.protocol.buf, MSGB)
        self.assertEqual(b.protocol.buf, MSGA)

    def testStreamLossy(self, num = 2**12, prob=0.5):
        self.testStreamSimple(num, prob)

class SimpleReactor(unittest.TestCase):
    def setUp(self):
        self.noisy = 0
        self.a = makeReceiver(2020)
        self.b = makeReceiver(2021)
        self.ac = self.a.connectionForAddr(('127.0.0.1', 2021))
        self.bc = self.b.connectionForAddr(('127.0.0.1', 2020))
        self.ac.noisy = self.noisy
        self.bc.noisy = self.noisy
    def testSimple(self):
        msg = "Testing 1, 2, 3"
        self.ac.write(msg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.bc.state, confirmed)
        self.assertEqual(self.bc.protocol.q, [msg])

class SimpleReactorEcho(unittest.TestCase):
    def setUp(self):
        self.noisy = 0
        self.a = makeReceiver(2022)
        self.b = makeEcho(2023)
        self.ac = self.a.connectionForAddr(('127.0.0.1', 2023))
        self.bc = self.b.connectionForAddr(('127.0.0.1', 2022))
    def testSimple(self):
        msg = "Testing 1, 2, 3"
        self.ac.write(msg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.ac.protocol.q, [msg])
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.ac.protocol.q, [msg])


class SimpleReactorStream(unittest.TestCase):
    def setUp(self):
        self.noisy = 0
        self.a = makeStreamReceiver(2024)
        self.b = makeStreamReceiver(2025)
        self.ac = self.a.connectionForAddr(('127.0.0.1', 2025))
        self.bc = self.b.connectionForAddr(('127.0.0.1', 2024))
    def testSimple(self):
        msg = "Testing 1, 2, 3"
        self.ac.write(msg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.bc.protocol.buf, msg)
        
class SimpleReactorStreamBig(unittest.TestCase):
    def setUp(self):
        self.noisy = 0
        self.a = makeStreamReceiver(2026)
        self.b = makeStreamReceiver(2027)
        self.ac = self.a.connectionForAddr(('127.0.0.1', 2027))
        self.bc = self.b.connectionForAddr(('127.0.0.1', 2026))
    def testBig(self):
        msg = open('/dev/urandom').read(4096)
        self.ac.write(msg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.bc.protocol.buf, msg)

class EchoReactorStreamBig(unittest.TestCase):
    def setUp(self):
        self.noisy = 0
        self.a = makeStreamReceiver(2028)
        self.b = makeEcho(2029)
        self.ac = self.a.connectionForAddr(('127.0.0.1', 2029))
    def testBig(self):
        msg = open('/dev/urandom').read(256)
        self.ac.write(msg)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.assertEqual(self.ac.protocol.buf, msg)

        
