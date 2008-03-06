
"""The main apt-p2p modules.

To run apt-p2p, you probably want to do something like::

  from apt_p2p.apt_p2p import AptP2P
  myapp = AptP2P(myDHT)

where myDHT is a DHT that implements interfaces.IDHT.

Diagram of the interaction between the given modules::
  
  +---------------+    +-----------------------------------+    +-------------
  |     AptP2P    |    |               DHT                 |    |  Internet
  |               |--->|join                            DHT|----|--\    
  |               |--->|loadConfig                         |    |  | Another
  |               |--->|getValue                           |    |  | Node
  |               |--->|storeValue                      DHT|<---|--/
  |               |--->|leave                              |    |
  |               |    +-----------------------------------+    |
  |               |    +-------------+    +----------------+    |
  |               |    | PeerManager |    | HTTPDownloader*|    |
  |               |--->|get          |--->|get         HTTP|----|---> Mirror
  |               |    |             |--->|getRange        |    |
  |               |--->|close        |--->|close       HTTP|----|--\       
  |               |    +-------------+    +----------------+    |  | Another
  |               |    +-----------------------------------+    |  | Peer
  |               |    |           HTTPServer          HTTP|<---|--/   
  |               |--->|getHTTPFactory                     |    +-------------
  |check_freshness|<---|                                   |    +-------------
  |       get_resp|<---|                               HTTP|<---|HTTP Request
  |               |    +-----------------------------------+    | 
  |               |    +---------------+    +--------------+    | Local Net
  |               |    | CacheManager  |    | ProxyFile-   |    | (apt)
  |               |--->|scanDirectories|    | Stream*      |    |
  |               |--->|save_file      |--->|__init__  HTTP|--->|HTTP Response
  |               |--->|save_error     |    |              |    +-------------
  |               |    |               |    |              |    +-------------
  |new_cached_file|<---|               |    |          file|--->|write file
  |               |    +---------------+    +--------------+    |
  |               |    +---------------+    +--------------+    | Filesystem
  |               |    | MirrorManager |    | AptPackages* |    |
  |               |--->|updatedFile    |--->|file_updated  |    | 
  |               |--->|findHash       |--->|findHash  file|<---|read file
  +---------------+    +---------------+    +--------------+    +-------------

"""
