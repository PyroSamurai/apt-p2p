## Copyright 2002 Andrew Loewenstern, All Rights Reserved

from socket import *

# simple UDP communicator

class Listener:
    def __init__(self, host, port):
	self.msgq = []
	self.sock = socket(AF_INET, SOCK_DGRAM)
	self.sock.setblocking(0)
	self.sock.bind((host, port))
	    
    def qMsg(self, msg, host, port):
	self.msgq.append((msg, host, port))
    
    def qLen(self):
	return len(self.msgq)
    
    def dispatchMsg(self):
	if self.qLen() > 0:
	    msg, host, port = self.msgq[0]
	    del self.msgq[0]
	    self.sock.sendto(msg, 0, (host, port))
	
    def receiveMsg(self):
	msg = ()
	try:
	    msg = self.sock.recvfrom(65536)
	except error, tup:
	    if tup[1] == "Resource temporarily unavailable":
		# no message
		return msg
	    print error, tup
	else:
	    return msg
	    
    def __del__(self):
	self.sock.close()



###########################
import unittest

class ListenerTest(unittest.TestCase):
    def setUp(self):
	self.a = Listener('localhost', 8080)
	self.b = Listener('localhost', 8081)
    def tearDown(self):
	del(self.a)
	del(self.b)
	
    def testQueue(self):
	assert self.a.qLen() == 0, "expected queue to be empty"
	self.a.qMsg('hello', 'localhost', 8081)
	assert self.a.qLen() == 1, "expected one message to be in queue"
	self.a.qMsg('hello', 'localhost', 8081)
	assert self.a.qLen() == 2, "expected two messages to be in queue"
	self.a.dispatchMsg()
	assert self.a.qLen() == 1, "expected one message to be in queue"
	self.a.dispatchMsg()
	assert self.a.qLen() == 0, "expected all messages to be flushed from queue"

    def testSendReceiveOne(self):
	self.a.qMsg('hello', 'localhost', 8081)
	self.a.dispatchMsg()
	
	assert self.b.receiveMsg()[0] == "hello", "did not receive expected message"
	assert self.b.receiveMsg() == (), "received unexpected message"
 
	self.b.qMsg('hello', 'localhost', 8080)
	self.b.dispatchMsg()
	
	assert self.a.receiveMsg()[0] == "hello", "did not receive expected message"
		
	assert self.a.receiveMsg() == (), "received unexpected message"

    def testSendReceiveInterleaved(self):
	self.a.qMsg('hello', 'localhost', 8081)
	self.a.qMsg('hello', 'localhost', 8081)
	self.a.dispatchMsg()
	self.a.dispatchMsg()
	
	assert self.b.receiveMsg()[0] == "hello", "did not receive expected message"
	assert self.b.receiveMsg()[0] == "hello", "did not receive expected message"		
	assert self.b.receiveMsg() == (), "received unexpected message"
 
	self.b.qMsg('hello', 'localhost', 8080)
	self.b.qMsg('hello', 'localhost', 8080)
	self.b.dispatchMsg()
	self.b.dispatchMsg()
	
	assert self.a.receiveMsg()[0] == "hello", "did not receive expected message"
	assert self.a.receiveMsg()[0] == "hello", "did not receive expected message"		
	assert self.a.receiveMsg() == (), "received unexpected message"
    

if __name__ == '__main__':
    unittest.main()
