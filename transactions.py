import messages
from dispatcher import Transaction

class TransactionFactory:
    def __init__(self, id, dispatcher):
	self.id = id
	self.dispatcher = dispatcher
	self.mf = messages.MessageFactory(self.id)
		
    def Ping(self, node, response, default):
	""" create a ping transaction """
	t = Transaction(self.dispatcher, node, response, default)
	str = self.mf.encodePing(t.id)
	t.setPayload(str)
	t.setResponseTemplate(messages.PONG)
	return t

    def FindNode(self, target, key, response, default):
	""" find node query """
	t = Transaction(self.dispatcher, target, response, default)
	str = self.mf.encodeFindNode(t.id, key)
	t.setPayload(str)
	t.setResponseTemplate(messages.GOT_NODES)
	return t

    def StoreValue(self, target, key, value, response, default):
	""" find node query """
	t = Transaction(self.dispatcher, target, response, default)
	str = self.mf.encodeStoreValue(t.id, key, value)
	t.setPayload(str)
	t.setResponseTemplate(messages.STORED_VALUE)
	return t

    def GetValue(self, target, key, response, default):
	""" find value query, response is GOT_VALUES or GOT_NODES! """
	t = Transaction(self.dispatcher, target, response, default)
	str = self.mf.encodeGetValue(t.id, key)
	t.setPayload(str)
	t.setResponseTemplate(messages.GOT_NODES_OR_VALUES)
	return t

    def Pong(self, ping_t):
	""" create a pong response to ping transaction """
	t = Transaction(self.dispatcher, ping_t.target, None, None, ping_t.id)
	str = self.mf.encodePong(t.id)
	t.setPayload(str)
	return t

    def GotNodes(self, findNode_t, nodes):
	""" respond with gotNodes """
	t = Transaction(self.dispatcher, findNode_t.target, None, None, findNode_t.id)
	str = self.mf.encodeGotNodes(t.id, nodes)
	t.setPayload(str)
	return t

    def GotValues(self, findNode_t, values):
	""" respond with gotNodes """
	t = Transaction(self.dispatcher, findNode_t.target, None, None, findNode_t.id)
	str = self.mf.encodeGotValues(t.id, values)
	t.setPayload(str)
	return t
 
    def StoredValue(self, tr):
	""" store value response, really just a pong """
	t = Transaction(self.dispatcher, tr.target, None, None, id = tr.id)
	str = self.mf.encodeStoredValue(t.id)
	t.setPayload(str)
	return t


###########
