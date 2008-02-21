## Copyright 2002-2004 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

from twisted.python import log

from node import Node, NULL_ID

class KNodeBase(Node):
    def checkSender(self, dict):
        try:
            senderid = dict['rsp']['id']
        except KeyError:
            log.msg("No peer id in response")
            raise Exception, "No peer id in response."
        else:
            if self.id != NULL_ID and senderid != self.id:
                log.msg("Got response from different node than expected.")
                self.table.invalidateNode(self)
                
        return dict

    def errBack(self, err):
        log.err(err)
        return err
        
    def ping(self, id):
        df = self.conn.sendRequest('ping', {"id":id})
        df.addErrback(self.errBack)
        df.addCallback(self.checkSender)
        return df
    
    def join(self, id):
        df = self.conn.sendRequest('join', {"id":id})
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
    def storeValue(self, key, value, token, id):
        df = self.conn.sendRequest('store_value', {"key" : key, "value" : value, "token" : token, "id": id})
        df.addErrback(self.errBack)
        df.addCallback(self.checkSender)
        return df
