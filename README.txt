[Note: I have no idea how up to date the code examples are -- icepick ]

Khashmir is a distributed hash table that uses an XOR distance metric and a
routing table similar to Kademlia [1].  Use Khashmir to build distributed
applications.  Note that Khashmir currently isn't very attack resistant.

  1 - http://kademlia.scs.cs.nyu.edu/
  
Khashmir is implemented in Python using the Twisted [2] asynchronous networking
framework.  Network deliver is done using Airhook [3].  PySQLite is used for each
peer's backing store of keys and values.  The backing store is currently
held in memory but it could easily be placed on disk.  Values expire after
24 hours, by default.  Each peer stores multiple values for a key and
currently returns all available values when requested.
  
  2 - http://twistedmatrix.com
  3 - http://airhook.org/

If you just want to watch it build a test network of peers, run "python
khashmir.py <num peers>" This script will create the specified number of
peers, give each one three random contacts, tell each one to find the
closest nodes, then pick a random peer and have it find another random peer
(ten times), then pick a random peer to insert a key/value and have three
random peers try to find it (ten times.)

quick example:

>>> k = khashmir.test_one('<ip>', 4444)  # choose any port

- which is the same thing as - 

>>> import thread, khashmir
>>> k = khashmir.Khashmir('<ip>', <port>) # choose any port
>>> thread.start_new_thread(k.app.run, ())


If you want to make another peer in the same session, use 

>>> peer = khashmir.Khashmir(host, port) 

then do 

>>> peer.app.run() 

to register with the already running thread.


Now you need some contacts.  Add as some contacts and try to find close
nodes in the network.

>>> k.addContact('127.0.0.1', 8080)  # locate another peer
>>> k.findCloseNodes()  # query the network to bootstrap our table

Keys are always 20-character strings (sha1 hashes) currently there is no
limit to value sizes, something will go in ASAP

Now try storing and retrieving values.

# callback returns node info dictionaries and actual connection information
# for peers that accepted the value. Currently finds the K closest nodes and
# makes one attempt to store them
>>> k.storeValueForKey(key, value, callback=None)  

# callback returns lists of values, will fire multiple times, last time will
# be an empty list
>>> k.valueForKey(key, callback)



TWEAKABLE SETTINGS:

ktable.py: 
	K - the size of routing table buckets, how many nodes to return
	    in response to find-node/value request, and how many nodes to issue
	    storeValue to; default is 8, which should work for hundreds of
	    nodes, you might want to bump it if you think your network is going
	    to be large

A bunch of other tweakables are in const.py

