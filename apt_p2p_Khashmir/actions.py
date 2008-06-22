
"""Details of how to perform actions on remote peers."""

from datetime import datetime

from twisted.internet import reactor, defer
from twisted.python import log

from khash import intify
from ktable import K
from util import uncompact

class ActionBase:
    """Base class for some long running asynchronous proccesses like finding nodes or values.
    
    @type caller: L{khashmir.Khashmir}
    @ivar caller: the DHT instance that is performing the action
    @type target: C{string}
    @ivar target: the target of the action, usually a DHT key
    @type config: C{dictionary}
    @ivar config: the configuration variables for the DHT
    @type action: C{string}
    @ivar action: the name of the action to call on remote nodes
    @type stats: L{stats.StatsLogger}
    @ivar stats: the statistics modules to report to
    @type num: C{long}
    @ivar num: the target key in integer form
    @type queried: C{dictionary}
    @ivar queried: the nodes that have been queried for this action,
        keys are node IDs, values are the node itself
    @type answered: C{dictionary}
    @ivar answered: the nodes that have answered the queries
    @type failed: C{dictionary}
    @ivar failed: the nodes that have failed to answer the queries
    @type found: C{dictionary}
    @ivar found: nodes that have been found so far by the action
    @type sorted_nodes: C{list} of L{node.Node}
    @ivar sorted_nodes: a sorted list of nodes by there proximity to the key
    @type results: C{dictionary}
    @ivar results: keys are the results found so far by the action
    @type desired_results: C{int}
    @ivar desired_results: the minimum number of results that are needed
        before the action should stop
    @type callback: C{method}
    @ivar callback: the method to call with the results
    @type outstanding: C{dictionary}
    @ivar outstanding: the nodes that have outstanding requests for this action,
        keys are node IDs, values are the number of outstanding results from the node
    @type outstanding_results: C{int}
    @ivar outstanding_results: the total number of results that are expected from
        the requests that are currently outstanding
    @type finished: C{boolean}
    @ivar finished: whether the action is done
    @type started: C{datetime.datetime}
    @ivar started: the time the action was started at
    @type sort: C{method}
    @ivar sort: used to sort nodes by their proximity to the target
    """
    
    def __init__(self, caller, target, callback, config, stats, action, num_results = None):
        """Initialize the action.
        
        @type caller: L{khashmir.Khashmir}
        @param caller: the DHT instance that is performing the action
        @type target: C{string}
        @param target: the target of the action, usually a DHT key
        @type callback: C{method}
        @param callback: the method to call with the results
        @type config: C{dictionary}
        @param config: the configuration variables for the DHT
        @type stats: L{stats.StatsLogger}
        @param stats: the statistics gatherer
        @type action: C{string}
        @param action: the name of the action to call on remote nodes
        @type num_results: C{int}
        @param num_results: the minimum number of results that are needed before
            the action should stop (optional, defaults to getting all the results)
        
        """
        
        self.caller = caller
        self.target = target
        self.config = config
        self.action = action
        self.stats = stats
        self.stats.startedAction(action)
        self.num = intify(target)
        self.queried = {}
        self.answered = {}
        self.failed = {}
        self.found = {}
        self.sorted_nodes = []
        self.results = {}
        self.desired_results = num_results
        self.callback = callback
        self.outstanding = {}
        self.outstanding_results = 0
        self.finished = False
        self.started = datetime.now()
    
        def sort(a, b, num=self.num):
            """Sort nodes relative to the ID we are looking for."""
            x, y = num ^ a.num, num ^ b.num
            if x > y:
                return 1
            elif x < y:
                return -1
            return 0
        self.sort = sort

    #{ Main operation
    def goWithNodes(self, nodes):
        """Start the action's process with a list of nodes to contact."""
        self.started = datetime.now()
        for node in nodes:
            self.found[node.id] = node
        self.sortNodes()
        self.schedule()
    
    def schedule(self):
        """Schedule requests to be sent to remote nodes."""
        if self.finished:
            return
        
        # Get the nodes to be processed
        nodes = self.getNodesToProcess()
        
        # Check if we are already done
        if nodes is None or (self.desired_results and
                             ((len(self.results) >= abs(self.desired_results)) or
                              (self.desired_results < 0 and
                               len(self.answered) >= self.config['STORE_REDUNDANCY']))):
            self.finished = True
            result = self.generateResult()
            reactor.callLater(0, self.callback, *result)
            return

        # Check if we have enough outstanding results coming
        if (self.desired_results and 
            len(self.results) + self.outstanding_results >= abs(self.desired_results)):
            return
        
        # Loop for each node that should be processed
        for node in nodes:
            # Don't send requests twice or to ourself
            if node.id not in self.queried:
                self.queried[node.id] = 1
                
                # Get the action to call on the node
                if node.id == self.caller.node.id:
                    try:
                        f = getattr(self.caller, 'krpc_' + self.action)
                    except AttributeError:
                        log.msg("%s doesn't have a %s method!" % (node, 'krpc_' + self.action))
                        continue
                else:
                    try:
                        f = getattr(node, self.action)
                    except AttributeError:
                        log.msg("%s doesn't have a %s method!" % (node, self.action))
                        continue

                # Get the arguments to the action's method
                try:
                    args, expected_results = self.generateArgs(node)
                except ValueError:
                    continue

                # Call the action on the remote node
                self.outstanding[node.id] = expected_results
                self.outstanding_results += expected_results
                df = defer.maybeDeferred(f, *args)
                reactor.callLater(0, df.addCallbacks,
                                  *(self.gotResponse, self.actionFailed),
                                  **{'callbackArgs': (node, ),
                                     'errbackArgs': (node, )})
                        
            # We might have to stop for now
            if (len(self.outstanding) >= self.config['CONCURRENT_REQS'] or
                (self.desired_results and
                 len(self.results) + self.outstanding_results >= abs(self.desired_results))):
                break
            
        assert self.outstanding_results >= 0

        # If no requests are outstanding, then we are done
        if len(self.outstanding) == 0:
            self.finished = True
            result = self.generateResult()
            reactor.callLater(0, self.callback, *result)

    def gotResponse(self, dict, node):
        """Receive a response from a remote node."""
        if self.finished or self.answered.has_key(node.id) or self.failed.has_key(node.id):
            # a day late and a dollar short
            return
        try:
            # Process the response
            self.processResponse(dict)
        except Exception, e:
            # Unexpected error with the response
            log.msg("action %s failed on %s/%s: %r" % (self.action, node.host, node.port, e))
            if node.id != self.caller.node.id:
                self.caller.nodeFailed(node)
            self.failed[node.id] = 1
        else:
            self.answered[node.id] = 1
            if node.id != self.caller.node.id:
                reactor.callLater(0, self.caller.insertNode, node)
        if self.outstanding.has_key(node.id):
            self.outstanding_results -= self.outstanding[node.id]
            del self.outstanding[node.id]
        self.schedule()

    def actionFailed(self, err, node):
        """Receive an error from a remote node."""
        log.msg("action %s failed on %s/%s: %s" % (self.action, node.host, node.port, err.getErrorMessage()))
        if node.id != self.caller.node.id:
            self.caller.nodeFailed(node)
        self.failed[node.id] = 1
        if self.outstanding.has_key(node.id):
            self.outstanding_results -= self.outstanding[node.id]
            del self.outstanding[node.id]
        self.schedule()
    
    def handleGotNodes(self, nodes):
        """Process any received node contact info in the response.
        
        Not called by default, but suitable for being called by
        L{processResponse} in a recursive node search.
        """
        if nodes and type(nodes) != list:
            raise ValueError, "got a malformed response, from bittorrent perhaps"
        for compact_node in nodes:
            node_contact = uncompact(compact_node)
            node = self.caller.Node(node_contact)
            if not self.found.has_key(node.id):
                self.found[node.id] = node

    def sortNodes(self):
        """Sort the nodes, if necessary.
        
        Assumes nodes are never removed from the L{found} dictionary.
        """
        if len(self.sorted_nodes) != len(self.found):
            self.sorted_nodes = self.found.values()
            self.sorted_nodes.sort(self.sort)
                
    #{ Subclass for specific actions
    def getNodesToProcess(self):
        """Generate a list of nodes to process next.
        
        This implementation is suitable for a recurring search over all nodes.
        It will stop the search when the closest K nodes have been queried.
        It also prematurely drops requests to nodes that have fallen way behind.
        
        @return: sorted list of nodes to query, or None if we are done
        """
        # Find the K closest nodes that haven't failed, count how many answered
        self.sortNodes()
        closest_K = []
        ans = 0
        for node in self.sorted_nodes:
            if node.id not in self.failed:
                closest_K.append(node)
                if node.id in self.answered:
                    ans += 1
                if len(closest_K) >= K:
                    break
        
        # If we have responses from the K closest nodes, then we are done
        if ans >= K:
            log.msg('Got the answers we need, aborting search')
            return None
        
        # Check the oustanding requests to see if they are still closest
        for id in self.outstanding.keys():
            if self.found[id] not in closest_K:
                # Request is not important, allow another to go
                log.msg("Request to %s/%s is taking too long, moving on" %
                        (self.found[id].host, self.found[id].port))
                self.outstanding_results -= self.outstanding[id]
                del self.outstanding[id]

        return closest_K
    
    def generateArgs(self, node):
        """Generate the arguments to the node's action.
        
        Also return the number of results expected from this request.
        
        @raise ValueError: if the node should not be queried
        """
        return (self.caller.node.id, self.target), 0
    
    def processResponse(self, dict):
        """Process the response dictionary received from the remote node."""
        self.handleGotNodes(dict['nodes'])

    def generateResult(self, nodes):
        """Create the final result to return to the L{callback} function."""
        self.stats.completedAction(self.action, self.started)
        return []
        

class FindNode(ActionBase):
    """Find the closest nodes to the key."""

    def __init__(self, caller, target, callback, config, stats, action="find_node"):
        ActionBase.__init__(self, caller, target, callback, config, stats, action)

    def processResponse(self, dict):
        """Save the token received from each node."""
        if dict["id"] in self.found:
            self.found[dict["id"]].updateToken(dict.get('token', ''))
        self.handleGotNodes(dict['nodes'])

    def generateResult(self):
        """Result is the K closest nodes to the target."""
        self.sortNodes()
        self.stats.completedAction(self.action, self.started)
        closest_K = []
        for node in self.sorted_nodes:
            if node.id not in self.failed:
                closest_K.append(node)
                if len(closest_K) >= K:
                    break
        return (closest_K, )
    

class FindValue(ActionBase):
    """Find the closest nodes to the key and check for values."""

    def __init__(self, caller, target, callback, config, stats, action="find_value"):
        ActionBase.__init__(self, caller, target, callback, config, stats, action)

    def processResponse(self, dict):
        """Save the number of values each node has."""
        if dict["id"] in self.found:
            self.found[dict["id"]].updateNumValues(dict.get('num', 0))
        self.handleGotNodes(dict['nodes'])
        
    def generateResult(self):
        """Result is the nodes that have values, sorted by proximity to the key."""
        self.sortNodes()
        self.stats.completedAction(self.action, self.started)
        return ([node for node in self.sorted_nodes if node.num_values > 0], )
    

class GetValue(ActionBase):
    """Retrieve values from a list of nodes."""
    
    def __init__(self, caller, target, num_results, callback, config, stats, action="get_value"):
        ActionBase.__init__(self, caller, target, callback, config, stats, action, num_results)

    def getNodesToProcess(self):
        """Nodes are never added, always return the same sorted node list."""
        return self.sorted_nodes
    
    def generateArgs(self, node):
        """Arguments include the number of values to request."""
        if node.num_values > 0:
            # Request all desired results from each node, just to be sure.
            num_values = abs(self.desired_results) - len(self.results)
            assert num_values > 0
            if num_values > node.num_values:
                num_values = 0
            return (self.caller.node.id, self.target, num_values), node.num_values
        else:
            raise ValueError, "Don't try and get values from this node because it doesn't have any"

    def processResponse(self, dict):
        """Save the returned values, calling the L{callback} each time there are new ones."""
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

    def generateResult(self):
        """Results have all been returned, now send the empty list to end the action."""
        self.stats.completedAction(self.action, self.started)
        return (self.target, [])
        

class StoreValue(ActionBase):
    """Store a value in a list of nodes."""

    def __init__(self, caller, target, value, num_results, callback, config, stats, action="store_value"):
        """Initialize the action with the value to store.
        
        @type value: C{string}
        @param value: the value to store in the nodes
        """
        ActionBase.__init__(self, caller, target, callback, config, stats, action, num_results)
        self.value = value
        
    def getNodesToProcess(self):
        """Nodes are never added, always return the same sorted list."""
        return self.sorted_nodes

    def generateArgs(self, node):
        """Args include the value to store and the node's token."""
        if node.token:
            return (self.caller.node.id, self.target, self.value, node.token), 1
        else:
            raise ValueError, "Don't store at this node since we don't know it's token"

    def processResponse(self, dict):
        """Save the response, though it should be nothin but the ID."""
        self.results[dict["id"]] = dict
    
    def generateResult(self):
        """Return all the response IDs received."""
        self.stats.completedAction(self.action, self.started)
        return (self.target, self.value, self.results.values())
