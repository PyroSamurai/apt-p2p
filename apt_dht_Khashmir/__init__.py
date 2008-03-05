
"""The apt-dht implementation of the Khashmir DHT.

These modules implement a modified Khashmir, which is a kademlia-like
Distributed Hash Table available at::

  http://khashmir.sourceforge.net/

The protocol for the implementation's communication is described here::

  http://www.camrdale.org/apt-dht/protocol.html

To run the DHT you probably want to do something like::

  from apt_dht_Khashmir import DHT
  myDHT = DHT.DHT()
  myDHT.loadConfig(config, section)
  myDHT.join()

at which point you should be up and running and connected to others in the DHT.

"""
