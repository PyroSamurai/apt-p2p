
from twisted.web2 import server, http, http_headers

from apt_dht_conf import config
from HTTPServer import TopLevel

class AptDHT:
    def __init__(self):
        self.http_server = TopLevel(config.defaults()['cache_dir'], self)
        self.http_site = server.Site(self.http_server)
    
    def getSite(self):
        return self.http_site
    
    def check_freshness(self, path, modtime, resp):
        return resp
    
    def get_resp(self, path):
        return http.Response(
            200,
            {'content-type': http_headers.MimeType('text', 'html')},
            """<html><body>
            <h2>Statistics</h2>
            <p>TODO: eventually this will cause a P2P lookup.</body></html>""")
