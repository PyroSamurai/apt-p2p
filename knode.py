## Copyright 2002-2004 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

from node import Node
from twisted.internet.defer import Deferred
from const import reactor, NULL_ID


class IDChecker:
    def __init__(id):
        self.id = id

class KNodeBase(Node):
    def checkSender(self, dict):
        try:
            senderid = dict['rsp']['id']
        except KeyError:
            print ">>>> No peer id in response"
            raise Exception, "No peer id in response."
        else:
            if self.id != NULL_ID and senderid != self.id:
                print "Got response from different node than expected."
                self.table.invalidateNode(self)
                
        return dict

    def errBack(self, err):
        print ">>> ", err
        return err
        
    def ping(self, id):
        df = self.conn.sendRequest('ping', {"id":id})
        df.addErrback(self.errBack)
        df.addCallback(self.checkSender)
        return df
    def findNode(self, target, id):
        df = self.conn.sendRequest('find_node', {"target" : target, "id": id})
        df.addErrback(self.errBack)
        df.addCallback(self.checkSender)
        return df

class KNodeRead(KNodeBase):
    def findValue(self, key, id):
        df =  self.conn.sendRequest('find_value', {"key" : key, "id" : id})
        df.addErrback(self.errBack)
        df.addCallback(self.checkSender)
        return df

class KNodeWrite(KNodeRead):
    def storeValue(self, key, value, id):
        df = self.conn.sendRequest('store_value', {"key" : key, "value" : value, "id": id})
        df.addErrback(self.errBack)
        df.addCallback(self.checkSender)
        return df
    def storeValues(self, key, value, id):
        df = self.conn.sendRequest('store_values', {"key" : key, "values" : value, "id": id})
        df.addErrback(self.errBack)
        df.addCallback(self.checkSender)
        return df
