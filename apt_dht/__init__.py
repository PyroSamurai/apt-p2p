
"""The main apt-dht modules.

Diagram of the interaction between the given modules::
  
  +---------------+    +-----------------------------------+    +-------------
  |     AptDHT    |    |               DHT                 |    |  Internet
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
