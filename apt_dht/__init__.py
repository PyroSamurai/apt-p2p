
"""The main apt-dht modules.

Diagram of the interaction between the given modules::
  
  +---------------+    +-----------------------------------+    +-------------
  |     AptDHT    |    |               DHT                 |    |  Internet
  |               |--->|join                            DHT|----|--\    
  |               |--->|loadConfig                         |    |  | Another
  |               |--->|getValue                           |    |  | Peer
  |               |--->|storeValue                      DHT|<---|--/
  |               |--->|leave                              |    |
  |               |    +-----------------------------------+    |
  |               |    +-------------+    +----------------+    |
  |               |    | PeerManager |    | HTTPDownloader*|    |
  |               |--->|get          |--->|get         HTTP|----|---> Mirror
  |               |    |             |--->|getRange        |    |
  |               |--->|close        |--->|close       HTTP|----|--\
  |               |    +-------------+    +----------------+    |  |
  |               |    +-----------------------------------+    |  | Another
  |               |    |           HTTPServer              |    |  | Peer
  |               |--->|getHTTPFactory                 HTTP|<---|--/
  |check_freshness|<---|                                   |    +-------------
  |       get_resp|<---|                                   |    +-------------
  |          /----|--->|setDirectories                 HTTP|<---|HTTP Request
  |          |    |    +-----------------------------------+    | 
  |          |    |    +---------------+    +--------------+    | Local Net
  |          |    |    | CacheManager  |    | ProxyFile-   |    | (apt)
  |          |    |--->|scanDirectories|    | Stream*      |    |
  | setDirectories|<---|               |--->|__init__  HTTP|--->|HTTP Response
  |               |--->|save_file      |    |              |    +-------------
  |               |--->|save_error     |    |              |    +-------------
  |new_cached_file|<---|               |    |          file|--->|write file
  |               |    +---------------+    +--------------+    |
  |               |    +---------------+    +--------------+    | Filesystem
  |               |    | MirrorManager |    | AptPackages* |    |
  |               |--->|updatedFile    |--->|file_updated  |--->|write file
  |               |--->|findHash       |--->|findHash      |    |
  +---------------+    +---------------+    +--------------+    +-------------

"""
