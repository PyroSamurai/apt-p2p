from node import Node
from twisted.internet.defer import Deferred
from const import reactor, NULL_ID

class KNode(Node):
    def makeResponse(self, df):
        """ Make our callback cover that checks to make sure the id of the response is the same as what we are expecting """
        def _callback(dict, d=df):
            try:
                senderid = dict['sender']['id']
            except KeyError:
                d.errback()
            else:
                if self.id != NULL_ID and senderid != self._senderDict['id']:
                    d.errback()
                else:
                    d.callback(dict)
        return _callback
        
    def ping(self, sender):
        return self.conn.protocol.sendRequest('ping', {"sender":sender})
    def findNode(self, target, sender):
        return self.conn.protocol.sendRequest('find_node', {"target" : target, "sender": sender})
    def storeValue(self, key, value, sender):
        return self.conn.protocol.sendRequest('store_value', {"key" : key, "value" : value, "sender": sender})
    def findValue(self, key, sender):
        return self.conn.protocol.sendRequest('find_value', {"key" : key, "sender" : sender})