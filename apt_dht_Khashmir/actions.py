## Copyright 2002-2004 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

from twisted.internet import reactor
from twisted.python import log

from khash import intify
from util import uncompact

class ActionBase:
    """ base class for some long running asynchronous proccesses like finding nodes or values """
    def __init__(self, caller, target, callback, config):
        self.caller = caller
        self.target = target
        self.config = config
        self.num = intify(target)
        self.found = {}
        self.queried = {}
        self.answered = {}
        self.callback = callback
        self.outstanding = 0
        self.finished = 0
    
        def sort(a, b, num=self.num):
            """ this function is for sorting nodes relative to the ID we are looking for """
            x, y = num ^ a.num, num ^ b.num
            if x > y:
                return 1
            elif x < y:
                return -1
            return 0
        self.sort = sort
        
    def actionFailed(self, err, node):
        log.msg("action %s failed (%s) %s/%s" % (self.__class__.__name__, self.config['PORT'], node.host, node.port))
        log.err(err)
        self.caller.table.nodeFailed(node)
        self.outstanding = self.outstanding - 1
        self.schedule()
    
    def goWithNodes(self, t):
        pass
    

class FindNode(ActionBase):
    """ find node action merits it's own class as it is a long running stateful process """
    def handleGotNodes(self, dict):
        _krpc_sender = dict['_krpc_sender']
        dict = dict['rsp']
        n = self.caller.Node(dict["id"], _krpc_sender[0], _krpc_sender[1])
        self.caller.insertNode(n)
        if dict["id"] in self.found:
            self.found[dict["id"]].updateToken(dict.get('token', ''))
        l = dict["nodes"]
        if self.finished or self.answered.has_key(dict["id"]):
            # a day late and a dollar short
            return
        self.outstanding = self.outstanding - 1
        self.answered[dict["id"]] = 1
        for compact_node in l:
            node = uncompact(compact_node)
            n = self.caller.Node(node)
            if not self.found.has_key(n.id):
                self.found[n.id] = n
        self.schedule()
        
    def schedule(self):
        """
            send messages to new peers, if necessary
        """
        if self.finished:
            return
        l = self.found.values()
        l.sort(self.sort)
        for node in l[:self.config['K']]:
            if node.id == self.target:
                self.finished=1
                return self.callback([node])
            if (not self.queried.has_key(node.id)) and node.id != self.caller.node.id:
                #xxxx t.timeout = time.time() + FIND_NODE_TIMEOUT
                df = node.findNode(self.target, self.caller.node.id)
                df.addCallbacks(self.handleGotNodes, self.actionFailed, errbackArgs = (node, ))
                self.outstanding = self.outstanding + 1
                self.queried[node.id] = 1
            if self.outstanding >= self.config['CONCURRENT_REQS']:
                break
        assert self.outstanding >=0
        if self.outstanding == 0:
            ## all done!!
            self.finished=1
            reactor.callLater(0, self.callback, l[:self.config['K']])
    
    def goWithNodes(self, nodes):
        """
            this starts the process, our argument is a transaction with t.extras being our list of nodes
            it's a transaction since we got called from the dispatcher
        """
        for node in nodes:
            if node.id == self.caller.node.id:
                continue
            else:
                self.found[node.id] = node
        
        self.schedule()
    

class FindValue(ActionBase):
    def handleGotNodes(self, dict):
        _krpc_sender = dict['_krpc_sender']
        dict = dict['rsp']
        n = self.caller.Node(dict["id"], _krpc_sender[0], _krpc_sender[1])
        self.caller.insertNode(n)
        if dict["id"] in self.found:
            self.found[dict["id"]].updateNumValues(dict.get('num', 0))
        l = dict["nodes"]
        if self.finished or self.answered.has_key(dict["id"]):
            # a day late and a dollar short
            return
        self.outstanding = self.outstanding - 1
        self.answered[dict["id"]] = 1
        for compact_node in l:
            node = uncompact(compact_node)
            n = self.caller.Node(node)
            if not self.found.has_key(n.id):
                self.found[n.id] = n
        self.schedule()
        
    def schedule(self):
        """
            send messages to new peers, if necessary
        """
        if self.finished:
            return
        l = self.found.values()
        l.sort(self.sort)
        for node in l[:self.config['K']]:
            if (not self.queried.has_key(node.id)) and node.id != self.caller.node.id:
                df = node.findValue(self.target, self.caller.node.id)
                df.addCallbacks(self.handleGotNodes, self.actionFailed, errbackArgs = (node, ))
                self.outstanding = self.outstanding + 1
                self.queried[node.id] = 1
            if self.outstanding >= self.config['CONCURRENT_REQS']:
                break
        assert self.outstanding >=0
        if self.outstanding == 0:
            ## all done!!
            self.finished=1
            l = [node for node in self.found.values() if node.num_values > 0]
            reactor.callLater(0, self.callback, l)
    
    def goWithNodes(self, nodes):
        """
            this starts the process, our argument is a transaction with t.extras being our list of nodes
            it's a transaction since we got called from the dispatcher
        """
        for node in nodes:
            if node.id == self.caller.node.id:
                continue
            else:
                self.found[node.id] = node
        
        self.schedule()


class GetValue(ActionBase):
    def __init__(self, caller, target, num, callback, config, action="getValue"):
        ActionBase.__init__(self, caller, target, callback, config)
        self.num_values = num
        self.outstanding_gets = 0
        self.action = action
        
    def gotValues(self, dict, node):
        dict = dict['rsp']
        self.outstanding -= 1
        self.caller.insertNode(node)
        if self.finished:
            return
        if dict.has_key('values'):
            def x(y, z=self.results):
                if not z.has_key(y):
                    z[y] = 1
                    return y
                else:
                    return None
            z = len(dict['values'])
            v = filter(None, map(x, dict['values']))
            if len(v):
                reactor.callLater(0, self.callback, self.target, v)
        if len(self.results) >= self.num_values:
            self.finished=1
            reactor.callLater(0, self.callback, self.target, [])
        else:
            if not len(self.results) + self.outstanding_gets >= self.num_values:
                self.schedule()
    
    def schedule(self):
        if self.finished:
            return
        
        for node in self.nodes:
            if node.id not in self.queried and node.id != self.caller.node.id and node.num_values > 0:
                try:
                    f = getattr(node, self.action)
                except AttributeError:
                    log.msg("%s doesn't have a %s method!" % (node, self.action))
                else:
                    self.outstanding += 1
                    self.outstanding_gets += node.num_values
                    df = f(self.target, 0, self.caller.node.id)
                    df.addCallbacks(self.gotValues, self.actionFailed, callbackArgs = (node, ), errbackArgs = (node, ))
                    self.queried[node.id] = 1
            if len(self.results) + self.outstanding_gets >= self.num_values or \
                self.outstanding >= self.config['CONCURRENT_REQS']:
                break
        assert self.outstanding >=0
        if self.outstanding == 0:
            self.finished = 1
            reactor.callLater(0, self.callback, self.target, [])
                    
    def goWithNodes(self, nodes, found = None):
        self.results = {}
        if found:
            for n in found:
                self.results[n] = 1
        self.nodes = nodes
        self.nodes.sort(self.sort)
        self.schedule()


class StoreValue(ActionBase):
    def __init__(self, caller, target, value, callback, config, action="storeValue"):
        ActionBase.__init__(self, caller, target, callback, config)
        self.value = value
        self.stored = []
        self.action = action
        
    def storedValue(self, t, node):
        self.outstanding -= 1
        self.caller.insertNode(node)
        if self.finished:
            return
        self.stored.append(t)
        if len(self.stored) >= self.config['STORE_REDUNDANCY']:
            self.finished=1
            self.callback(self.target, self.value, self.stored)
        else:
            if not len(self.stored) + self.outstanding >= self.config['STORE_REDUNDANCY']:
                self.schedule()
        return t
    
    def schedule(self):
        if self.finished:
            return
        num = self.config['CONCURRENT_REQS'] - self.outstanding
        if num > self.config['STORE_REDUNDANCY']:
            num = self.config['STORE_REDUNDANCY']
        for i in range(num):
            try:
                node = self.nodes.pop()
            except IndexError:
                if self.outstanding == 0:
                    self.finished = 1
                    self.callback(self.target, self.value, self.stored)
            else:
                if not node.id == self.caller.node.id:
                    self.outstanding += 1
                    try:
                        f = getattr(node, self.action)
                    except AttributeError:
                        log.msg("%s doesn't have a %s method!" % (node, self.action))
                    else:
                        df = f(self.target, self.value, node.token, self.caller.node.id)
                        df.addCallbacks(self.storedValue, self.actionFailed, callbackArgs = (node, ), errbackArgs = (node, ))
                    
    def goWithNodes(self, nodes):
        self.nodes = nodes
        self.nodes.sort(self.sort)
        self.schedule()
