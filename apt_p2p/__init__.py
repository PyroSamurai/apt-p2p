
"""The main apt-p2p modules.

To run apt-p2p, you probably want to do something like::

  from apt_p2p.apt_p2p import AptP2P
  factory = AptP2P(DHT)

where DHT is a class that implements interfaces.IDHT.

Diagram of the interaction between the given modules::
  
  +---------------+    +-----------------------------------+    +-------------
  |     AptP2P    |    |               DHT                 |    |  
  |               |--->|join                            DHT|----|--\    
  |               |--->|loadConfig                         |    |  | Another
  |               |--->|getValue                           |    |  | Node
  |               |--->|storeValue                      DHT|<---|--/
  |               |--->|leave                              |    |
  |         /-----|--->|getStats                           |    |
  |         |     |    +-----------------------------------+    | Internet
  |         |     |    +-------------+    +----------------+    |
  |         |     |    | PeerManager |    | HTTPDownloader*|    |
  |         |     |--->|get          |--->|get         HTTP|----|---> Mirror
  |         |     |    |             |--->|getRange        |    |
  |         |     |--->|close        |--->|close       HTTP|----|--\       
  |         |     |    +-------------+    +----------------+    |  | Another
  |         |     |    +-----------------------------------+    |  | Peer
  |         |     |    |           HTTPServer          HTTP|<---|--/   
  |         |     |    |                                   |    +-------------
  |       getStats|<---|                                   |    +-------------
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
