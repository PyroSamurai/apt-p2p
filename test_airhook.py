import unittest
from airhook import *
from random import uniform as rand

if __name__ =="__main__":
	tests = unittest.defaultTestLoader.loadTestsFromNames(['test_airhook'])
	result = unittest.TextTestRunner().run(tests)



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
		a.transport = StringIO()
		if not msg:
			a.sendNext()
	if noisy:
				print 6*dir + " " + pscope(msg)
	return msg
	

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
		self.a = AirhookConnection(StringIO(), (None, 'localhost', 4040), None)
		self.b = AirhookConnection(StringIO(), (None, 'localhost', 4040), None)
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
		self.assertEqual(b.state, sent)
		self.assertEqual(b.inSeq, 0)
		self.assertEqual(b.obSeq, 0)
		msg = swap(b, '<', self.noisy)		
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
		self.a = AirhookConnection(StringIO(), (None, 'localhost', 4040), None)
		self.b = AirhookConnection(StringIO(), (None, 'localhost', 4040), None)
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
		self.assertEqual(len(b.imsgq), 1)
		self.assertEqual(b.imsgq[0], TESTMSG)
		
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
		self.assertEqual(len(b.imsgq), 1)
		self.assertEqual(b.imsgq[0], TESTMSG2)
		self.assertEqual(b.weMissed, [(1, 0)])
		msg = swap(b, '<', self.noisy)
		
		a.datagramReceived(msg)
								
		msg = swap(a, '>', self.noisy)
		
		b.datagramReceived(msg)
		self.assertEqual(len(b.imsgq), 2)
		b.imsgq.sort()
		l = [TESTMSG2, TESTMSG]
		l.sort()
		self.assertEqual(b.imsgq,l)
		
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

		self.assertEqual(len(b.imsgq), 2)
		b.imsgq.sort()
		l = [TESTMSG2, TESTMSG]
		l.sort()
		self.assertEqual(b.imsgq,l)

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
		self.assertEqual(len(b.imsgq), 1)
		self.assertEqual(b.imsgq[0], TESTMSG)
		self.assertEqual(b.inMsg, 1)
		self.assertEqual(b.weMissed, [])
		msg = swap(b, '<', self.noisy)
		
		a.datagramReceived(msg)
		assert(a.outMsgs[1] != None)
		msg = swap(a, '>', self.noisy)

		b.datagramReceived(msg)
		self.assertEqual(b.state, confirmed)
		self.assertEqual(len(b.imsgq), 1)
		self.assertEqual(b.imsgq[0], TESTMSG)
		self.assertEqual(b.weMissed, [(2, 1)])
		msg = swap(b, '<', self.noisy)

		a.datagramReceived(msg)
		msg = swap(a, '>', self.noisy)

		b.datagramReceived(msg)
		self.assertEqual(len(b.imsgq), 2)
		b.imsgq.sort()
		l = [TESTMSG2, TESTMSG]
		l.sort()
		self.assertEqual(b.imsgq,l)
		
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

		self.assertEqual(len(b.imsgq), 2)
		b.imsgq.sort()
		l = [TESTMSG2, TESTMSG]
		l.sort()
		self.assertEqual(b.imsgq,l)

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
		self.assertEqual(b.state, sent)
		
		msg = swap(b, '<', self.noisy)
		a.datagramReceived(msg)

		msg = swap(a, '>', self.noisy)

		b.datagramReceived(msg)
		self.assertEqual(len(b.imsgq), 2)
		l = [TESTMSGA, TESTMSGB]
		l.sort();b.imsgq.sort()
		self.assertEqual(b.imsgq, l)
		self.assertEqual(b.inMsg, 2)

		msg = swap(b, '<', self.noisy)
		a.datagramReceived(msg)
		
		self.assertEqual(len(a.imsgq), 2)
		l = [TESTMSGC, TESTMSGD]
		l.sort();a.imsgq.sort()
		self.assertEqual(a.imsgq, l)
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
		
		while a.state != confirmed or b.state != confirmed or ord(msga[0]) & FLAG_NEXT or ord(msgb[0]) & FLAG_NEXT :
			msga = swap(a, '>', self.noisy)
	
			if rand(0,1) < prob:
				b.datagramReceived(msga)
			
			msgb = swap(b, '<', self.noisy)

			if rand(0,1) < prob:
				a.datagramReceived(msgb)

		self.assertEqual(a.state, confirmed)
		self.assertEqual(b.state, confirmed)
		self.assertEqual(len(b.imsgq), 2)
		l = [TESTMSGA, TESTMSGB]
		l.sort();b.imsgq.sort()
		self.assertEqual(b.imsgq, l)
				
		self.assertEqual(len(a.imsgq), 2)
		l = [TESTMSGC, TESTMSGD]
		l.sort();a.imsgq.sort()
		self.assertEqual(a.imsgq, l)

	def testOneWayBlast(self, num = 2**12):
		a = self.a
		b = self.b
		import sha
		
		
		for i in xrange(num):
			a.omsgq.append(sha.sha(`i`).digest())
		msga = swap(a, '>', self.noisy)
		while a.omsgq or b.omsgq or a.weMissed or b.weMissed or ord(msga[0]) & (FLAG_NEXT | FLAG_MISSED) or ord(msgb[0]) & (FLAG_NEXT | FLAG_MISSED):
			b.datagramReceived(msga)
			msgb = swap(b, '<', self.noisy)

			a.datagramReceived(msgb)
			msga = swap(a, '>', self.noisy)

		self.assertEqual(len(b.imsgq), num)
		
	def testTwoWayBlast(self, num = 2**12, prob=0.5):
		a = self.a
		b = self.b
		import sha
		
		
		for i in xrange(num):
			a.omsgq.append(sha.sha('a' + `i`).digest())
			b.omsgq.append(sha.sha('b' + `i`).digest())
			
		while a.omsgq or b.omsgq or a.weMissed or b.weMissed or ord(msga[0]) & (FLAG_NEXT | FLAG_MISSED) or ord(msgb[0]) & (FLAG_NEXT | FLAG_MISSED):
			if rand(0,1) < prob:
				msga = swap(a, '>', self.noisy)
				b.datagramReceived(msga)
			else:
				msga = swap(a, '>', 0)
			if rand(0,1) < prob:
				msgb = swap(b, '<', self.noisy)
				a.datagramReceived(msgb)
			else:
				msgb = swap(b, '<', 0)
					


		self.assertEqual(len(a.imsgq), num)
		self.assertEqual(len(b.imsgq), num)
		
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

class StreamTests(unittest.TestCase):
	def setUp(self):
		self.noisy = 0
		class queuer:
			def __init__(self):
				self.msg = ""
			def dataCameIn(self, host, port, data):
				self.msg+= data
		self.A = queuer()
		self.B = queuer()
		self.a = StreamConnection(StringIO(), (None, 'localhost', 4040), self.A)
		self.b = StreamConnection(StringIO(), (None, 'localhost', 4040), self.B)

	def testStreamSimple(self, num = 2**18, prob=1.0):
		f = open('/dev/urandom', 'r')
		a = self.a
		b = self.b
		A = self.A
		B = self.B

		MSGA = f.read(num)
		MSGB = f.read(num)
		self.a.sendSomeData(MSGA)
		self.b.sendSomeData(MSGB)
		
		while a.omsgq or b.omsgq or a.weMissed or b.weMissed or ord(msga[0]) & (FLAG_NEXT | FLAG_MISSED) or ord(msgb[0]) & (FLAG_NEXT | FLAG_MISSED):
			if rand(0,1) < prob:
				msga = swap(a, '>', self.noisy)
				b.datagramReceived(msga)
			else:
				msga = swap(a, '>', 0)
			if rand(0,1) < prob:
				msgb = swap(b, '<', self.noisy)
				a.datagramReceived(msgb)
			else:
				msgb = swap(b, '<', 0)
		self.assertEqual(len(self.A.msg), len(MSGB))
		self.assertEqual(len(self.B.msg), len(MSGA))
		self.assertEqual(self.A.msg, MSGB)
		self.assertEqual(self.B.msg, MSGA)

	def testStreamLossy(self, num = 2**18, prob=0.5):
		self.testStreamSimple(num, prob)
