from node import Node
from twisted.internet.defer import Deferred
from xmlrpcclient import XMLRPCClientFactory as factory
from const import reactor, NULL_ID

class KNode(Node):
	def makeResponse(self, df):
		""" Make our callback cover that checks to make sure the id of the response is the same as what we are expecting """
		def _callback(args, d=df):
			try:
				l, sender = args
			except:
				d.callback(args)
			else:
				if self.id != NULL_ID and sender['id'] != self._senderDict['id']:
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
		f = factory('find_node', (target.encode('base64'), sender), self.makeResponse(df), df.errback)
		reactor.connectTCP(self.host, self.port, f)
		return df
	def storeValue(self, key, value, sender):
		df = Deferred()
		f = factory('store_value', (key.encode('base64'), value.encode('base64'), sender), self.makeResponse(df), df.errback)
		reactor.connectTCP(self.host, self.port, f)
		return df
	def findValue(self, key, sender):
		df = Deferred()
		f = factory('find_value', (key.encode('base64'), sender), self.makeResponse(df), df.errback)
		reactor.connectTCP(self.host, self.port, f)
		return df
