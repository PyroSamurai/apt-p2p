from twisted.internet.protocol import ClientFactory
from twisted.protocols.http import HTTPClient
from twisted.internet.defer import Deferred

from xmlrpclib import loads, dumps

USER_AGENT = 'Python/Twisted XMLRPC 0.1'
class XMLRPCClient(HTTPClient):
    def connectionMade(self):
	payload = dumps(self.args, self.method)
	self.sendCommand('POST', '/RPC2')
	self.sendHeader('User-Agent', USER_AGENT)
	self.sendHeader('Content-Type', 'text/xml')
	self.sendHeader('Content-Length', len(payload))
	self.endHeaders()
	self.transport.write(payload)
        self.transport.write('\r\n')
	
    def handleResponse(self, buf):
	args, name = loads(buf)
	apply(self.d.callback, args)


class XMLRPCClientFactory(ClientFactory):
    def __init__(self, method, args, callback=None, errback=None):
	self.method = method
	self.args = args
	self.d = Deferred()
	if callback:
	    self.d.addCallback(callback)
	if errback:
	    self.d.addErrback(errback)
	    
    def buildProtocol(self, addr):
        prot =  XMLRPCClient()
	prot.method = self.method
	prot.args = self.args
	prot.d = self.d
	return prot
