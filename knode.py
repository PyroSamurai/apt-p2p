from node import Node
from twisted.internet.defer import Deferred
from xmlrpcclient import XMLRPCClientFactory as factory
from const import reactor
from xmlrpclib import Binary


class KNode(Node):	
    def ping(self, sender):
	df = Deferred()
	f = factory('ping', (sender,), df.callback, df.errback)
	reactor.connectTCP(self.host, self.port, f)
	return df
    def findNode(self, target, sender):
	df = Deferred()
	f = factory('find_node', (Binary(target), sender), df.callback, df.errback)
	reactor.connectTCP(self.host, self.port, f)
	return df
    def storeValue(self, key, value, sender):
	df = Deferred()
	f = factory('store_value', (Binary(key), Binary(value), sender), df.callback, df.errback)
	reactor.connectTCP(self.host, self.port, f)
	return df
    def findValue(self, key, sender):
	df = Deferred()
	f = factory('find_value', (Binary(key), sender), df.callback, df.errback)
	reactor.connectTCP(self.host, self.port, f)
	return df
