
from bz2 import BZ2Decompressor
from zlib import decompressobj, MAX_WBITS
from gzip import FCOMMENT, FEXTRA, FHCRC, FNAME, FTEXT
from urlparse import urlparse
import os

from twisted.python import log, filepath
from twisted.internet import defer
from twisted.trial import unittest
from twisted.web2 import stream
from twisted.web2.http import splitHostPort

from AptPackages import AptPackages

aptpkg_dir='.apt-dht'

DECOMPRESS_EXTS = ['.gz', '.bz2']
DECOMPRESS_FILES = ['release', 'sources', 'packages']

class MirrorError(Exception):
    """Exception raised when there's a problem with the mirror."""

class ProxyFileStream(stream.SimpleStream):
    """Saves a stream to a file while providing a new stream."""
    
    def __init__(self, stream, outFile, hash, decompress = None, decFile = None):
        """Initializes the proxy.
        
        @type stream: C{twisted.web2.stream.IByteStream}
        @param stream: the input stream to read from
        @type outFile: C{twisted.python.filepath.FilePath}
        @param outFile: the file to write to
        @type hash: L{Hash.HashObject}
        @param hash: the hash object to use for the file
        @type decompress: C{string}
        @param decompress: also decompress the file as this type
            (currently only '.gz' and '.bz2' are supported)
        @type decFile: C{twisted.python.filepath.FilePath}
        @param decFile: the file to write the decompressed data to
        """
        self.stream = stream
        self.outFile = outFile.open('w')
        self.hash = hash
        self.hash.new()
        self.gzfile = None
        self.bz2file = None
        if decompress == ".gz":
            self.gzheader = True
            self.gzfile = decFile.open('w')
            self.gzdec = decompressobj(-MAX_WBITS)
        elif decompress == ".bz2":
            self.bz2file = decFile.open('w')
            self.bz2dec = BZ2Decompressor()
        self.length = self.stream.length
        self.start = 0
        self.doneDefer = defer.Deferred()

    def _done(self):
        """Close the output file."""
        if not self.outFile.closed:
            self.outFile.close()
            self.hash.digest()
            if self.gzfile:
                data_dec = self.gzdec.flush()
                self.gzfile.write(data_dec)
                self.gzfile.close()
                self.gzfile = None
            if self.bz2file:
                self.bz2file.close()
                self.bz2file = None
                
            self.doneDefer.callback(self.hash)
    
    def read(self):
        """Read some data from the stream."""
        if self.outFile.closed:
            return None
        
        data = self.stream.read()
        if isinstance(data, defer.Deferred):
            data.addCallbacks(self._write, self._done)
            return data
        
        self._write(data)
        return data
    
    def _write(self, data):
        """Write the stream data to the file and return it for others to use."""
        if data is None:
            self._done()
            return data
        
        self.outFile.write(data)
        self.hash.update(data)
        if self.gzfile:
            if self.gzheader:
                self.gzheader = False
                new_data = self._remove_gzip_header(data)
                dec_data = self.gzdec.decompress(new_data)
            else:
                dec_data = self.gzdec.decompress(data)
            self.gzfile.write(dec_data)
        if self.bz2file:
            dec_data = self.bz2dec.decompress(data)
            self.bz2file.write(dec_data)
        return data
    
    def _remove_gzip_header(self, data):
        if data[:2] != '\037\213':
            raise IOError, 'Not a gzipped file'
        if ord(data[2]) != 8:
            raise IOError, 'Unknown compression method'
        flag = ord(data[3])
        # modtime = self.fileobj.read(4)
        # extraflag = self.fileobj.read(1)
        # os = self.fileobj.read(1)

        skip = 10
        if flag & FEXTRA:
            # Read & discard the extra field, if present
            xlen = ord(data[10])
            xlen = xlen + 256*ord(data[11])
            skip = skip + 2 + xlen
        if flag & FNAME:
            # Read and discard a null-terminated string containing the filename
            while True:
                if not data[skip] or data[skip] == '\000':
                    break
                skip += 1
            skip += 1
        if flag & FCOMMENT:
            # Read and discard a null-terminated string containing a comment
            while True:
                if not data[skip] or data[skip] == '\000':
                    break
                skip += 1
            skip += 1
        if flag & FHCRC:
            skip += 2     # Read & discard the 16-bit header CRC
        return data[skip:]

    def close(self):
        """Clean everything up and return None to future reads."""
        self.length = 0
        self._done()
        self.stream.close()

class MirrorManager:
    """Manages all requests for mirror objects."""
    
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self.cache = filepath.FilePath(self.cache_dir)
        self.apt_caches = {}
    
    def extractPath(self, url):
        parsed = urlparse(url)
        host, port = splitHostPort(parsed[0], parsed[1])
        site = host + ":" + str(port)
        path = parsed[2]
            
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
    
    def updatedFile(self, url, file_path):
        site, baseDir, path = self.extractPath(url)
        self.init(site, baseDir)
        self.apt_caches[site][baseDir].file_updated(path, file_path)

    def findHash(self, url):
        site, baseDir, path = self.extractPath(url)
        if site in self.apt_caches and baseDir in self.apt_caches[site]:
            return self.apt_caches[site][baseDir].findHash(path)
        d = defer.Deferred()
        d.errback(MirrorError("Site Not Found"))
        return d
    
    def save_file(self, response, hash, url):
        """Save a downloaded file to the cache and stream it."""
        log.msg('Returning file: %s' % url)
        
        parsed = urlparse(url)
        destFile = self.cache.preauthChild(parsed[1] + parsed[2])
        log.msg('Saving returned %r byte file to cache: %s' % (response.stream.length, destFile.path))
        
        if destFile.exists():
            log.msg('File already exists, removing: %s' % destFile.path)
            destFile.remove()
        else:
            destFile.parent().makedirs()
            
        root, ext = os.path.splitext(destFile.basename())
        if root.lower() in DECOMPRESS_FILES and ext.lower() in DECOMPRESS_EXTS:
            ext = ext.lower()
            decFile = destFile.sibling(root)
            log.msg('Decompressing to: %s' % decFile.path)
            if decFile.exists():
                log.msg('File already exists, removing: %s' % decFile.path)
                decFile.remove()
        else:
            ext = None
            decFile = None
            
        orig_stream = response.stream
        response.stream = ProxyFileStream(orig_stream, destFile, hash, ext, decFile)
        response.stream.doneDefer.addCallback(self.save_complete, url, destFile,
                                              response.headers.getHeader('Last-Modified'),
                                              ext, decFile)
        response.stream.doneDefer.addErrback(self.save_error, url)
        return response

    def save_complete(self, hash, url, destFile, modtime = None, ext = None, decFile = None):
        """Update the modification time and AptPackages."""
        if modtime:
            os.utime(destFile.path, (modtime, modtime))
            if ext:
                os.utime(decFile.path, (modtime, modtime))
        
        result = hash.verify()
        if result or result is None:
            if result:
                log.msg('Hashes match: %s' % url)
            else:
                log.msg('Hashed file to %s: %s' % (hash.hexdigest(), url))
                
            self.updatedFile(url, destFile.path)
            if ext:
                self.updatedFile(url[:-len(ext)], decFile.path)
        else:
            log.msg("Hashes don't match %s != %s: %s" % (hash.hexexpected(), hash.hexdigest(), url))

    def save_error(self, failure, url):
        """An error has occurred in downloadign or saving the file."""
        log.msg('Error occurred downloading %s' % url)
        log.err(failure)
        return failure

class TestMirrorManager(unittest.TestCase):
    """Unit tests for the mirror manager."""
    
    timeout = 20
    pending_calls = []
    client = None
    
    def setUp(self):
        self.client = MirrorManager('/tmp')
        
    def test_extractPath(self):
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
        self.packagesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "_main_.*Packages$" | tail -n 1').read().rstrip('\n')
        self.sourcesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "_main_.*Sources$" | tail -n 1').read().rstrip('\n')
        for f in os.walk('/var/lib/apt/lists').next()[2]:
            if f[-7:] == "Release" and self.packagesFile.startswith(f[:-7]):
                self.releaseFile = f
                break
        
        self.client.updatedFile('http://' + self.releaseFile.replace('_','/'), 
                                '/var/lib/apt/lists/' + self.releaseFile)
        self.client.updatedFile('http://' + self.releaseFile[:self.releaseFile.find('_dists_')+1].replace('_','/') +
                                self.packagesFile[self.packagesFile.find('_dists_')+1:].replace('_','/'), 
                                '/var/lib/apt/lists/' + self.packagesFile)
        self.client.updatedFile('http://' + self.releaseFile[:self.releaseFile.find('_dists_')+1].replace('_','/') +
                                self.sourcesFile[self.sourcesFile.find('_dists_')+1:].replace('_','/'), 
                                '/var/lib/apt/lists/' + self.sourcesFile)

        lastDefer = defer.Deferred()
        
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/binary-i386/Packages.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = 'http://' + self.releaseFile.replace('_','/')[:-7] + 'main/binary-i386/Packages.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

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
        self.client = None
        