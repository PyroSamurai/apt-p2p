
"""Manage the multiple mirrors that may be requested.

@var aptpkg_dir: the name of the directory to use for mirror files
"""

from urlparse import urlparse
import os

from twisted.python import log
from twisted.python.filepath import FilePath
from twisted.internet import defer
from twisted.trial import unittest
from twisted.web2.http import splitHostPort

from AptPackages import AptPackages

aptpkg_dir='apt-packages'

class MirrorError(Exception):
    """Exception raised when there's a problem with the mirror."""

class MirrorManager:
    """Manages all requests for mirror information.
    
    @type cache_dir: L{twisted.python.filepath.FilePath}
    @ivar cache_dir: the directory to use for storing all files
    @type apt_caches: C{dictionary}
    @ivar apt_caches: the avaliable mirrors
    """
    
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self.apt_caches = {}
    
    def extractPath(self, url):
        """Break the full URI down into the site, base directory and path.
        
        Site is the host and port of the mirror. Base directory is the
        directory to the mirror location (usually just '/debian'). Path is
        the remaining path to get to the file.
        
        E.g. http://ftp.debian.org/debian/dists/sid/binary-i386/Packages.bz2
        would return ('ftp.debian.org:80', '/debian', 
        '/dists/sid/binary-i386/Packages.bz2').
        
        @param url: the URI of the file's location on the mirror
        @rtype: (C{string}, C{string}, C{string})
        @return: the site, base directory and path to the file
        """
        # Extract the host and port
        parsed = urlparse(url)
        host, port = splitHostPort(parsed[0], parsed[1])
        site = host + ":" + str(port)
        path = parsed[2]

        # Try to find the base directory (most can be found this way)
        i = max(path.rfind('/dists/'), path.rfind('/pool/'))
        if i >= 0:
            baseDir = path[:i]
            path = path[i:]
        else:
            # Uh oh, this is not good
            log.msg("Couldn't find a good base directory for path: %s" % (site + path))
            
            # Try to find an existing cache that starts with this one
            # (fallback to using an empty base directory)
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
        """Make sure an L{AptPackages} exists for this mirror."""
        if site not in self.apt_caches:
            self.apt_caches[site] = {}
            
        if baseDir not in self.apt_caches[site]:
            site_cache = self.cache_dir.child(aptpkg_dir).child('mirrors').child(site + baseDir.replace('/', '_'))
            site_cache.makedirs
            self.apt_caches[site][baseDir] = AptPackages(site_cache)
    
    def updatedFile(self, url, file_path):
        """A file in the mirror has changed or been added.
        
        @see: L{AptPackages.PackageFileList.update_file}
        """
        site, baseDir, path = self.extractPath(url)
        self.init(site, baseDir)
        self.apt_caches[site][baseDir].file_updated(path, file_path)

    def findHash(self, url):
        """Find the hash for a given url.

        @param url: the URI of the file's location on the mirror
        @rtype: L{twisted.internet.defer.Deferred}
        @return: a deferred that will fire with the returned L{Hash.HashObject}
        """
        site, baseDir, path = self.extractPath(url)
        self.init(site, baseDir)
        if site in self.apt_caches and baseDir in self.apt_caches[site]:
            return self.apt_caches[site][baseDir].findHash(path)
        return defer.fail(MirrorError("Site Not Found"))
    
    def cleanup(self):
        for site in self.apt_caches.keys():
            for baseDir in self.apt_caches[site].keys():
                self.apt_caches[site][baseDir].cleanup()
                del self.apt_caches[site][baseDir]
            del self.apt_caches[site]
    
class TestMirrorManager(unittest.TestCase):
    """Unit tests for the mirror manager."""
    
    timeout = 20
    pending_calls = []
    client = None
    
    def setUp(self):
        self.client = MirrorManager(FilePath('/tmp/.apt-p2p'))
        
    def test_extractPath(self):
        """Test extracting the site and base directory from various mirrors."""
        site, baseDir, path = self.client.extractPath('http://ftp.us.debian.org/debian/dists/unstable/Release')
        self.failUnless(site == "ftp.us.debian.org:80", "no match: %s" % site)
        self.failUnless(baseDir == "/debian", "no match: %s" % baseDir)
        self.failUnless(path == "/dists/unstable/Release", "no match: %s" % path)

        site, baseDir, path = self.client.extractPath('http://ftp.us.debian.org:16999/debian/pool/d/dpkg/dpkg_1.2.1-1.tar.gz')
        self.failUnless(site == "ftp.us.debian.org:16999", "no match: %s" % site)
        self.failUnless(baseDir == "/debian", "no match: %s" % baseDir)
        self.failUnless(path == "/pool/d/dpkg/dpkg_1.2.1-1.tar.gz", "no match: %s" % path)

        site, baseDir, path = self.client.extractPath('http://debian.camrdale.org/dists/unstable/Release')
        self.failUnless(site == "debian.camrdale.org:80", "no match: %s" % site)
        self.failUnless(baseDir == "", "no match: %s" % baseDir)
        self.failUnless(path == "/dists/unstable/Release", "no match: %s" % path)

    def verifyHash(self, found_hash, path, true_hash):
        self.failUnless(found_hash.hexexpected() == true_hash, 
                    "%s hashes don't match: %s != %s" % (path, found_hash.hexexpected(), true_hash))

    def test_findHash(self):
        """Tests finding the hash of an index file, binary package, source package, and another index file."""
        # Find the largest index files that are for 'main'
        self.packagesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "_main_.*Packages$" | tail -n 1').read().rstrip('\n')
        self.sourcesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "_main_.*Sources$" | tail -n 1').read().rstrip('\n')
        
        # Find the Release file corresponding to the found Packages file
        for f in os.walk('/var/lib/apt/lists').next()[2]:
            if f[-7:] == "Release" and self.packagesFile.startswith(f[:-7]):
                self.releaseFile = f
                break
        
        # Add all the found files to the mirror
        self.client.updatedFile('http://' + self.releaseFile.replace('_','/'), 
                                FilePath('/var/lib/apt/lists/' + self.releaseFile))
        self.client.updatedFile('http://' + self.releaseFile[:self.releaseFile.find('_dists_')+1].replace('_','/') +
                                self.packagesFile[self.packagesFile.find('_dists_')+1:].replace('_','/'), 
                                FilePath('/var/lib/apt/lists/' + self.packagesFile))
        self.client.updatedFile('http://' + self.releaseFile[:self.releaseFile.find('_dists_')+1].replace('_','/') +
                                self.sourcesFile[self.sourcesFile.find('_dists_')+1:].replace('_','/'), 
                                FilePath('/var/lib/apt/lists/' + self.sourcesFile))

        lastDefer = defer.Deferred()
        
        # Lookup a Packages.bz2 file
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/binary-i386/Packages.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = 'http://' + self.releaseFile.replace('_','/')[:-7] + 'main/binary-i386/Packages.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

        # Lookup the binary 'dpkg' package
        pkg_hash = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^SHA1:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')
        pkg_path = 'http://' + self.releaseFile[:self.releaseFile.find('_dists_')+1].replace('_','/') + \
                   os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^Filename:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')

        d = self.client.findHash(pkg_path)
        d.addCallback(self.verifyHash, pkg_path, pkg_hash)

        # Lookup the source 'dpkg' package
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
            src_path = 'http://' + self.releaseFile[:self.releaseFile.find('_dists_')+1].replace('_','/') + src_dir + '/' + src_paths[i]
            d = self.client.findHash(src_path)
            d.addCallback(self.verifyHash, src_path, src_hashes[i])
            
        # Lookup a Sources.bz2 file
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/source/Sources.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = 'http://' + self.releaseFile.replace('_','/')[:-7] + 'main/source/Sources.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

        d.addBoth(lastDefer.callback)
        return lastDefer

    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.client.cleanup()
        self.client = None
        