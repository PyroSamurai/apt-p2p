quick example:

>>> import khashmir
>>> k = khashmir.test_one(4444)  # choose any port

If you want to make another peer in the same session, use peer = khashmir.Khashmir(host, port) then do peer.app.run() to register with the already running thread.


>>> k.addContact('127.0.0.1', 8080)  # locate another peer
>>> k.findCloseNodes()  # query the network to bootstrap our table

Keys are always 20-character strings (sha1 hashes)

>>> k.storeValueForKey(key, value)  # no callback right now
>>> k.valueForKey(key, callback)

