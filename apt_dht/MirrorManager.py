
import os

from twisted.python import log
from twisted.internet import defer
from twisted.trial import unittest

from AptPackages import AptPackages

aptpkg_dir='.apt-dht'

class MirrorManager:
    """Manages all requests for mirror objects."""
    
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self.apt_caches = {}
    
    def extractPath(self, path):
        site, path = path.split('/',1)
        if not site:
            site, path = path.split('/',1)
        path = '/'+path
        
        # Make sure a port is included for consistency
        if site.find(':') < 0:
            site = site + ":80"
            
        i = max(path.rfind('/dists/'), path.rfind('/pool/'))
        if i >= 0:
            baseDir = path[:i]
            path = path[i:]
        else:
            # Uh oh, this is not good
            log.msg("Couldn't find a good base directory for path: %s" % (site + path))
            baseDir = ''
            if site in self.apt_caches:
                longest_match = 0
                for base in self.apt_caches[site]:
                    base_match = ''
                    for dirs in path.split('/'):
                        if base.startswith(base_match + '/' + dirs):
                            base_match += '/' + dirs
                        else:
                            break
                    if len(base_match) > longest_match:
                        longest_match = len(base_match)
                        baseDir = base_match
            log.msg("Settled on baseDir: %s" % baseDir)
        
        return site, baseDir, path
        
    def init(self, site, baseDir):
        if site not in self.apt_caches:
            self.apt_caches[site] = {}
            
        if baseDir not in self.apt_caches[site]:
            site_cache = os.path.join(self.cache_dir, aptpkg_dir, 'mirrors', site + baseDir.replace('/', '_'))
            self.apt_caches[site][baseDir] = AptPackages(site_cache)
    
    def updatedFile(self, path, file_path):
        site, baseDir, path = self.extractPath(path)
        self.init(site, baseDir)
        self.apt_caches[site][baseDir].file_updated(path, file_path)
    
    def findHash(self, path):
        site, baseDir, path = self.extractPath(path)
        if site in self.apt_caches and baseDir in self.apt_caches[site]:
            return self.apt_caches[site][baseDir].findHash(path)
        d = defer.Deferred()
        d.errback("Not Found")
        return d

class TestMirrorManager(unittest.TestCase):
    """Unit tests for the mirror manager."""
    
    pending_calls = []
    client = None
    
    def setUp(self):
        self.client = MirrorManager('/tmp')
        
    def test_extractPath(self):
        site, baseDir, path = self.client.extractPath('/ftp.us.debian.org/debian/dists/unstable/Release')
        self.failUnless(site == "ftp.us.debian.org:80", "no match: %s" % site)
        self.failUnless(baseDir == "/debian", "no match: %s" % baseDir)
        self.failUnless(path == "/dists/unstable/Release", "no match: %s" % path)

        site, baseDir, path = self.client.extractPath('/ftp.us.debian.org:16999/debian/pool/d/dpkg/dpkg_1.2.1-1.tar.gz')
        self.failUnless(site == "ftp.us.debian.org:16999", "no match: %s" % site)
        self.failUnless(baseDir == "/debian", "no match: %s" % baseDir)
        self.failUnless(path == "/pool/d/dpkg/dpkg_1.2.1-1.tar.gz", "no match: %s" % path)

    def verifyHash(self, found_hash, path, true_hash):
        self.failUnless(found_hash[0] == true_hash, 
                    "%s hashes don't match: %s != %s" % (path, found_hash[0], true_hash))

    def test_findHash(self):
        self.packagesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "_main_.*Packages$" | tail -n 1').read().rstrip('\n')
        self.sourcesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "_main_.*Sources$" | tail -n 1').read().rstrip('\n')
        for f in os.walk('/var/lib/apt/lists').next()[2]:
            if f[-7:] == "Release" and self.packagesFile.startswith(f[:-7]):
                self.releaseFile = f
                break
        
        self.client.updatedFile('/' + self.releaseFile.replace('_','/'), 
                                '/var/lib/apt/lists/' + self.releaseFile)
        self.client.updatedFile('/' + self.releaseFile[:self.releaseFile.find('_dists_')+1].replace('_','/') +
                                self.packagesFile[self.packagesFile.find('_dists_')+1:].replace('_','/'), 
                                '/var/lib/apt/lists/' + self.packagesFile)
        self.client.updatedFile('/' + self.releaseFile[:self.releaseFile.find('_dists_')+1].replace('_','/') +
                                self.sourcesFile[self.sourcesFile.find('_dists_')+1:].replace('_','/'), 
                                '/var/lib/apt/lists/' + self.sourcesFile)

        lastDefer = defer.Deferred()
        
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/binary-i386/Packages.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = '/' + self.releaseFile.replace('_','/')[:-7] + 'main/binary-i386/Packages.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

        pkg_hash = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^SHA1:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')
        pkg_path = '/' + self.releaseFile[:self.releaseFile.find('_dists_')+1].replace('_','/') + \
                   os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^Filename:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')

        d = self.client.findHash(pkg_path)
        d.addCallback(self.verifyHash, pkg_path, pkg_hash)

        src_dir = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -E "^Directory:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')
        src_hashes = os.popen('grep -A 20 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -A 4 -E "^Files:" | grep -E "^ " ' + 
                            ' | cut -d\  -f 2').read().split('\n')[:-1]
        src_paths = os.popen('grep -A 20 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -A 4 -E "^Files:" | grep -E "^ " ' + 
                            ' | cut -d\  -f 4').read().split('\n')[:-1]

        for i in range(len(src_hashes)):
            src_path = '/' + self.releaseFile[:self.releaseFile.find('_dists_')+1].replace('_','/') + src_dir + '/' + src_paths[i]
            d = self.client.findHash(src_path)
            d.addCallback(self.verifyHash, src_path, src_hashes[i])
            
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/source/Sources.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = '/' + self.releaseFile.replace('_','/')[:-7] + 'main/source/Sources.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

        d.addBoth(lastDefer.callback)
        return lastDefer

    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.client = None
        