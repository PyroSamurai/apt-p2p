##  Airhook Protocol http://airhook.org/protocol.html
##  Copyright 2002, Andrew Loewenstern, All Rights Reserved

from random import uniform as rand
from struct import pack, unpack
from time import time
from StringIO import StringIO
import unittest
from bisect import insort_left

from twisted.internet import protocol
from twisted.internet import reactor

# flags
FLAG_AIRHOOK = 128
FLAG_OBSERVED = 16
FLAG_SESSION = 8
FLAG_MISSED = 4
FLAG_NEXT = 2
FLAG_INTERVAL = 1

MAX_PACKET_SIZE = 1480

pending = 0
sent = 1
confirmed = 2

class Delegate:
	def setDelegate(self, delegate):
		self.delegate = delegate
	def getDelegate(self):
		return self.delegate
	def msgDelegate(self, method, args=(), kwargs={}):
		if hasattr(self, 'delegate') and hasattr(self.delegate, method) and callable(getattr(self.delegate, method)):
			apply(getattr(self.delegate, method) , args, kwargs)

class Airhook(protocol.DatagramProtocol):

	def __init__(self, connection_class):
		self.connection_class = connection_class
	def startProtocol(self):
		self.connections = {}
				
	def datagramReceived(self, datagram, addr):
		flag = datagram[0]
		if not flag & FLAG_AIRHOOK:  # first bit always must be 0
			conn = self.connectionForAddr(addr)
			conn.datagramReceieved(datagram)

	def connectionForAddr(self, addr):
		if not self.connections.has_key(addr):
			conn = connection_class(self.transport, addr, self.delegate)
			self.connections[addr] = conn
		return self.connections[addr]

		
class AirhookPacket:
	def __init__(self, msg):
		self.datagram = msg
		self.oseq =  ord(msg[1])
		self.seq = unpack("!H", msg[2:4])[0]
		self.flags = ord(msg[0])
		self.session = None
		self.observed = None
		self.next = None
		self.missed = []
		self.msgs = []
		skip = 4
		if self.flags & FLAG_OBSERVED:
			self.observed = unpack("!L", msg[skip:skip+4])[0]
			skip += 4
		if self.flags & FLAG_SESSION:
			self.session =  unpack("!L", msg[skip:skip+4])[0]
			skip += 4
		if self.flags & FLAG_NEXT:
			self.next =  ord(msg[skip])
			skip += 1
		if self.flags & FLAG_MISSED:
			num = ord(msg[skip]) + 1
			skip += 1
			for i in range(num):
				self.missed.append( ord(msg[skip+i]))
			skip += num
		if self.flags & FLAG_NEXT:
			while len(msg) - skip > 0:
				n = ord(msg[skip]) + 1
				skip+=1
				self.msgs.append( msg[skip:skip+n])
				skip += n

class AirhookConnection(Delegate):
	def __init__(self, transport, addr, delegate):
		self.delegate = delegate
		self.addr = addr
		type, self.host, self.port = addr
		self.transport = transport
		
		self.outSeq = 0  # highest sequence we have sent, can't be 255 more than obSeq
		self.obSeq = -1   # highest sequence confirmed by remote
		self.inSeq = 0   # last received sequence
		self.observed = None  # their session id
		self.sessionID = long(rand(0, 2**32))  # our session id
		
		self.lastTransmit = -1  # time we last sent a packet with messages
		self.lastReceieved = 0 # time we last received a packet with messages
		self.lastTransmitSeq = -1 # last sequence we sent a packet
		self.state = pending
		
		self.outMsgs = [None] * 256  # outgoing messages  (seq sent, message), index = message number
		self.outMsgNums = [None] * 256 # outgoing message numbers i = outNum % 256
		self.next = -1  # next outgoing message number
		self.omsgq = [] # list of messages to go out
		self.imsgq = [] # list of messages coming in
		self.sendSession = None  # send session/observed fields until obSeq > sendSession

		self.resetMessages()
	
	def resetMessages(self):
		self.weMissed = []
		self.inMsg = 0   # next incoming message number
	
	def datagramReceived(self, datagram):
		if not datagram:
			return
		response = 0 # if we know we have a response now (like resending missed packets)
		p = AirhookPacket(datagram)
		
		# check for state change
		if self.state == pending:
			if p.observed != None and p.session != None:
				if p.observed == self.sessionID:
					self.observed = p.session
					self.state = confirmed
				else:
					# bogus packet!
					return
			elif p.session != None:
				self.observed = p.session
				self.state = sent
				response = 1
		elif self.state == sent:
			if p.observed != None and p.session != None:
				if p.observed == self.sessionID:
					self.observed = p.session
					self.sendSession = self.outSeq
					self.state = confirmed
			if p.session != None:
				if not self.observed:
					self.observed = p.session
				elif self.observed != p.session:
					self.state = pending
					self.resetMessages()
					self.inSeq = p.seq
			response = 1
		elif self.state == confirmed:
			if p.session != None or p.observed != None :
				if p.session != self.observed or p.observed != self.sessionID:
					self.state = pending
					if seq == 0:
						self.resetMessages()
						self.inSeq = p.seq
	
		if self.state != pending:	
			msgs = []		
			missed = []

			# check to make sure sequence number isn't out of wack
			assert (p.seq - self.inSeq) % 2**16 < 256
			
			# see if they need us to resend anything
			for i in p.missed:
				response = 1
				if self.outMsgs[i] != None:
					self.omsgq.insert(0, self.outMsgs[i])
					self.outMsgs[i] = None
					
			# see if we need them to send anything
			if p.next != None:
				if p.next == 0 and self.inMsg == -1:
					missed = 255
				missed_count = (p.next - self.inMsg) % 256
				if missed_count:
					self.lastReceived = time()
					for i in range(missed_count):
						missed += [(self.outSeq, (self.inMsg + i) % 256)]
					response = 1
					self.weMissed += missed
				self.inMsg = (p.next + len(p.msgs)) % 256
				
			self.imsgq += p.msgs
			self.inSeq = p.seq
			
		if self.state == confirmed:
			# unpack the observed sequence
			tseq = unpack('!H', pack('!H', self.outSeq)[0] +  chr(p.oseq))[0]
			if ((self.outSeq - tseq)) % 2**16 > 255:
				tseq = unpack('!H', chr(ord(pack('!H', self.outSeq)[0]) - 1) + chr(p.oseq))[0]
			self.obSeq = tseq

		if response:
			reactor.callLater(0, self.sendNext)
		self.lastReceived = time()
		self.dataCameIn()
		
	def sendNext(self):
		flags = 0
		header = chr(self.inSeq & 255) + pack("!H", self.outSeq)
		ids = ""
		missed = ""
		msgs = ""
		
		if self.state == pending:
			flags = flags | FLAG_SESSION
			ids +=  pack("!L", self.sessionID)
			self.state = sent
		elif self.state == sent:
			if self.observed != None:
				flags = flags | FLAG_SESSION | FLAG_OBSERVED
				ids +=  pack("!LL", self.observed, self.sessionID)
			else:
				flags = flags | FLAG_SESSION
				ids +=  pack("!L", self.sessionID)

		else:
			if self.state == sent or self.sendSession:
				if self.state == confirmed and (self.obSeq - self.sendSession) % 2**16 < 256:
					self.sendSession = None
				else:
					flags = flags | FLAG_SESSION | FLAG_OBSERVED
					ids +=  pack("!LL", self.observed, self.sessionID)
		
		if self.obSeq >= 0:
			self.weMissed = filter(lambda a: a[0] > self.obSeq, self.weMissed)

		if self.weMissed:
			flags = flags | FLAG_MISSED
			missed += chr(len(self.weMissed) - 1)
			for i in self.weMissed:
				missed += chr(i[1])
				
		if self.state == confirmed and self.omsgq:
			first = (self.next + 1) % 256
			while len(self.omsgq) and (len(self.omsgq[-1]) + len(msgs) + len(missed) + len(ids) + len(header) + 1 <= MAX_PACKET_SIZE) :
				if self.obSeq == -1:
					highest = 0
				else:
					highest = self.outMsgNums[self.obSeq % 256]
				if self.next != -1 and (self.next + 1) % 256 == (highest - 1) % 256:
					break
				else:
					self.next = (self.next + 1) % 256
					msg = self.omsgq.pop()
					msgs += chr(len(msg) - 1) + msg
					self.outMsgs[self.next] = msg
		if msgs:
			flags = flags | FLAG_NEXT
			ids += chr(first)
			self.lastTransmitSeq = self.outSeq
			self.outMsgNums[self.outSeq % 256] = first
		else:
			if self.next == -1:
				self.outMsgNums[self.outSeq % 256] = 0
			else:
				self.outMsgNums[self.outSeq % 256] = self.next
			
		if (self.obSeq - self.lastTransmitSeq) % 2**16 > 256 and self.outMsgNums[self.obSeq % 256] != self.next and  not flags & FLAG_NEXT:
				flags = flags | FLAG_NEXT
				ids += chr((self.next + 1) % 256)
		packet = chr(flags) + header + ids + missed + msgs
		self.outSeq = (self.outSeq + 1) % 2**16
		self.lastTransmit = time()
		self.transport.write(packet)
		
		if self.omsgq and (self.next + 1) % 256 != self.outMsgNums[self.obSeq % 256] and (self.outSeq - self.obSeq) % 2**16 < 256:
			reactor.callLater(0, self.sendNext)
		else:
			reactor.callLater(1, self.sendNext)


	def dataCameIn(self):
		self.msgDelegate('dataCameIn', (self.host, self.port, self.imsgq))
		if hasattr(self, 'delegate') and self.delegate != None:
			self.imsgq = []

class ustr(str):
	def getseq(self):
		if not hasattr(self, 'seq'):
			self.seq = unpack("!H", self[0:2])[0]
		return self.seq
	def __lt__(self, other):
		return self.getseq() < other.getseq()
	def __le__(self, other):
		return self.getseq() <= other.getseq()
	def __eq__(self, other):
		return self.getseq() != other.getseq()
	def __ne__(self, other):
		return self.getseq() <= other.getseq()
	def __gt__(self, other):
		return self.getseq() > other.getseq()
	def __ge__(self, other):
		return self.getseq() >= other.getseq()

class OrderedConnection(AirhookConnection):
	def __init__(self, transport, addr, delegate):
		AirhookConnection.__init__(self, transport, addr, delegate)
		self.oseq = 0
		self.iseq = 0
		self.q = []

	def dataCameIn(self):
		# put 'em together
		for msg in self.imsgq:
			insort_left(self.q, ustr(msg))
		self.imsgq = []
		data = ""
		while self.q and self.iseq == self.q[0].getseq():
			data += self.q[0][2:]
			self.iseq = (self.iseq + 1) % 2**16
			self.q = self.q[1:]
		if data:
			self.msgDelegate('dataCameIn', (self.host, self.port, data))
		
	def sendSomeData(self, data):
		# chop it up and queue it up
		while data:
			p = "%s%s" % (pack("!H", self.oseq), data[:253])
			self.omsgq.append(p)
			data = data[253:]
			self.oseq = (self.oseq + 1) % 2**16

		if self.omsgq:
			self.sendNext()
