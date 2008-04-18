## Copyright 2002-2004 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

"""Represents a khashmir node in the DHT."""

from twisted.python import log

from node import Node, NULL_ID

class KNodeError(Exception):
    """An error occurred in contacting the node."""

class KNodeBase(Node):
    """A basic node that can only be pinged and help find other nodes."""
    
    def checkSender(self, dict):
        """Check the sender's info to make sure it meets expectations."""
        try:
            senderid = dict['id']
        except KeyError:
            log.msg("No peer id in response")
            raise KNodeError, "No peer id in response."
        else:
            if self.id != NULL_ID and senderid != self.id:
                log.msg("Got response from different node than expected.")
                self.table.invalidateNode(self)
                raise KNodeError, "Node ID has changed"
                
        return dict

    def ping(self, id):
        """Ping the node."""
        df = self.conn.sendRequest('ping', {"id":id})
        df.addCallback(self.checkSender)
        return df
    
    def join(self, id):
        """Use the node to bootstrap into the system."""
        df = self.conn.sendRequest('join', {"id":id})
        df.addCallback(self.checkSender)
        return df
    
    def find_node(self, id, target):
        """Request the nearest nodes to the target that the node knows about."""
        df = self.conn.sendRequest('find_node', {"target" : target, "id": id})
        df.addCallback(self.checkSender)
        return df

class KNodeRead(KNodeBase):
    """More advanced node that can also find and send values."""
    
    def find_value(self, id, key):
        """Request the nearest nodes to the key that the node knows about."""
        df =  self.conn.sendRequest('find_value', {"key" : key, "id" : id})
        df.addCallback(self.checkSender)
        return df

    def get_value(self, id, key, num):
        """Request the values that the node has for the key."""
        df = self.conn.sendRequest('get_value', {"key" : key, "num": num, "id" : id})
        df.addCallback(self.checkSender)
        return df

class KNodeWrite(KNodeRead):
    """Most advanced node that can also store values."""
    
    def store_value(self, id, key, value, token):
        """Store a value in the node."""
        df = self.conn.sendRequest('store_value', {"key" : key, "value" : value, "token" : token, "id": id})
        df.addCallback(self.checkSender)
        return df
