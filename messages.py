## Copyright 2002 Andrew Loewenstern, All Rights Reserved

from bencode import bencode, bdecode
from btemplate import *
from node import Node


# template checker for hash id
def hashid(thing, verbose):
    if type(thing) != type(''):
        raise ValueError, 'must be a string'
    if len(thing) != 20:
        raise ValueError, 'must be 20 characters long'

## our messages
BASE = compile_template({'id' : hashid, 'tid' : hashid, 'type' : string_template})

PING = compile_template({'id' : hashid, 'tid' : hashid, 'type' : 'ping'})
PONG = compile_template({'id' : hashid, 'tid' : hashid, 'type' : 'pong'})

FIND_NODE = compile_template({'id' : hashid, 'tid' : hashid, 'type' : 'find node', "target" : hashid})
GOT_NODES = compile_template({'id' : hashid, 'tid' : hashid, 'type' : 'got nodes', "nodes" : ListMarker({'id': hashid, 'host': string_template, 'port': 1})})

STORE_VALUE = compile_template({'id' : hashid, 'tid' : hashid, 'type' : 'store value', "key" : hashid, "value" : string_template})
STORED_VALUE = compile_template({'id' : hashid, 'tid' : hashid, 'type' : 'stored value'})

GET_VALUE = compile_template({'id' : hashid, 'tid' : hashid, 'type' : 'get value', "key" : hashid})
GOT_VALUES = compile_template({'id' : hashid, 'tid' : hashid, 'type' : 'got values', "values" : ListMarker({'key': hashid, 'value': string_template})})

GOT_NODES_OR_VALUES = compile_template([GOT_NODES, GOT_VALUES])


class MessageFactory:
    def __init__(self, id):
	self.id = id
	
    def encodePing(self, tid):
	return bencode({'id' : self.id, 'tid' : tid, 'type' : 'ping'})
    def decodePing(self, msg):
	msg = bdecode(msg)
	PING(msg)
	return msg
	
    def encodePong(self, tid):
	msg = {'id' : self.id, 'tid' : tid, 'type' : 'pong'}
	PONG(msg)
	return bencode(msg)
    def decodePong(self, msg):
	msg = bdecode(msg)
	PONG(msg)
	return msg

    def encodeFindNode(self, tid, target):
	return bencode({'id' : self.id, 'tid' : tid, 'type' : 'find node', 'target' : target})
    def decodeFindNode(self, msg):
	msg = bdecode(msg)
	FIND_NODE(msg)
	return msg

    def encodeStoreValue(self, tid, key, value):
	return bencode({'id' : self.id, 'tid' : tid, 'key' : key, 'type' : 'store value', 'value' : value})
    def decodeStoreValue(self, msg):
	msg = bdecode(msg)
	STORE_VALUE(msg)
	return msg
    
    
    def encodeStoredValue(self, tid):
	return bencode({'id' : self.id, 'tid' : tid, 'type' : 'stored value'})
    def decodeStoredValue(self, msg):
	msg = bdecode(msg)
	STORED_VALUE(msg)
	return msg

    
    def encodeGetValue(self, tid, key):
	return bencode({'id' : self.id, 'tid' : tid, 'key' : key, 'type' : 'get value'})
    def decodeGetValue(self, msg):
	msg = bdecode(msg)
	GET_VALUE(msg)
	return msg

    def encodeGotNodes(self, tid, nodes):
	n = []
	for node in nodes:
	    n.append({'id' : node.id, 'host' : node.host, 'port' : node.port})
	return bencode({'id' : self.id, 'tid' : tid, 'type' : 'got nodes', 'nodes' : n})
    def decodeGotNodes(self, msg):
	msg = bdecode(msg)
	GOT_NODES(msg)
	return msg

    def encodeGotValues(self, tid, values):
	n = []
	for value in values:
	    n.append({'key' : value[0], 'value' : value[1]})
	return bencode({'id' : self.id, 'tid' : tid, 'type' : 'got values', 'values' : n})
    def decodeGotValues(self, msg):
	msg = bdecode(msg)
	GOT_VALUES(msg)
	return msg
	
	

######
import unittest

class TestMessageEncoding(unittest.TestCase):
    def setUp(self):
	from sha import sha
	self.a = sha('a').digest()
	self.b = sha('b').digest()

    
    def test_ping(self):
	m = MessageFactory(self.a)
	s = m.encodePing(self.b)
	msg = m.decodePing(s)
	PING(msg)
	assert(msg['id'] == self.a)
	assert(msg['tid'] == self.b)
	
    def test_pong(self):
	m = MessageFactory(self.a)
	s = m.encodePong(self.b)
	msg = m.decodePong(s)
	PONG(msg)
	assert(msg['id'] == self.a)
	assert(msg['tid'] == self.b)
	
    def test_find_node(self):
	m = MessageFactory(self.a)
	s = m.encodeFindNode(self.a, self.b)
	msg = m.decodeFindNode(s)
	FIND_NODE(msg)
	assert(msg['id'] == self.a)
	assert(msg['tid'] == self.a)
	assert(msg['target'] == self.b)

    def test_store_value(self):
	m = MessageFactory(self.a)
	s = m.encodeStoreValue(self.a, self.b, 'foo')
	msg = m.decodeStoreValue(s)
	STORE_VALUE(msg)
	assert(msg['id'] == self.a)
	assert(msg['tid'] == self.a)
	assert(msg['key'] == self.b)
	assert(msg['value'] == 'foo')
	
    def test_stored_value(self):
	m = MessageFactory(self.a)
	s = m.encodeStoredValue(self.b)
	msg = m.decodeStoredValue(s)
	STORED_VALUE(msg)
	assert(msg['id'] == self.a)
	assert(msg['tid'] == self.b)
    
    def test_get_value(self):
	m = MessageFactory(self.a)
	s = m.encodeGetValue(self.a, self.b)
	msg = m.decodeGetValue(s)
	GET_VALUE(msg)
	assert(msg['id'] == self.a)
	assert(msg['tid'] == self.a)
	assert(msg['key'] == self.b)
    
    def test_got_nodes(self):
	m = MessageFactory(self.a)
	s = m.encodeGotNodes(self.a, [Node(self.b, 'localhost', 2002), Node(self.a, 'localhost', 2003)])
	msg = m.decodeGotNodes(s)
	GOT_NODES(msg)
	assert(msg['id'] == self.a)
	assert(msg['tid'] == self.a)
	assert(msg['nodes'][0]['id'] == self.b)
    
    def test_got_values(self):
	m = MessageFactory(self.a)
	s = m.encodeGotValues(self.a, [(self.b, 'localhost')])
	msg = m.decodeGotValues(s)
	GOT_VALUES(msg)
	assert(msg['id'] == self.a)
	assert(msg['tid'] == self.a)


if __name__ == "__main__":
	unittest.main()
