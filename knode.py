from node import Node
from twisted.internet.defer import Deferred
from xmlrpcclient import XMLRPCClientFactory as factory
from const import reactor, NULL_ID
from xmlrpclib import Binary


class KNode(Node):
    def makeResponse(self, df):
	def _callback(args, d=df):
	    try:
		l, sender = args
	    except:
		d.callback(args)
	    else:
		if self.id != NULL_ID and sender['id'].data != self.id:
		    d.errback()
		else:
		    d.callback(args)
	return _callback
    def ping(self, sender):
	df = Deferred()
	f = factory('ping', (sender,), self.makeResponse(df), df.errback)
	reactor.connectTCP(self.host, self.port, f)
	return df
    def findNode(self, target, sender):
	df = Deferred()
	f = factory('find_node', (Binary(target), sender), self.makeResponse(df), df.errback)
	reactor.connectTCP(self.host, self.port, f)
	return df
    def storeValue(self, key, value, sender):
	df = Deferred()
	f = factory('store_value', (Binary(key), Binary(value), sender), self.makeResponse(df), df.errback)
	reactor.connectTCP(self.host, self.port, f)
	return df
    def findValue(self, key, sender):
	df = Deferred()
	f = factory('find_value', (Binary(key), sender), self.makeResponse(df), df.errback)
	reactor.connectTCP(self.host, self.port, f)
	return df
