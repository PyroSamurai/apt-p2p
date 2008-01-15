
from bz2 import BZ2Decompressor
from zlib import decompressobj, MAX_WBITS
from gzip import FCOMMENT, FEXTRA, FHCRC, FNAME, FTEXT
from urlparse import urlparse
import os

from twisted.python import log
from twisted.python.filepath import FilePath
from twisted.internet import defer, reactor
from twisted.trial import unittest
from twisted.web2 import stream
from twisted.web2.http import splitHostPort

from Hash import HashObject

aptpkg_dir='apt-packages'

DECOMPRESS_EXTS = ['.gz', '.bz2']
DECOMPRESS_FILES = ['release', 'sources', 'packages']

class ProxyFileStream(stream.SimpleStream):
    """Saves a stream to a file while providing a new stream."""
    
    def __init__(self, stream, outFile, hash, decompress = None, decFile = None):
        """Initializes the proxy.
        
        @type stream: C{twisted.web2.stream.IByteStream}
        @param stream: the input stream to read from
        @type outFile: C{twisted.python.FilePath}
        @param outFile: the file to write to
        @type hash: L{Hash.HashObject}
        @param hash: the hash object to use for the file
        @type decompress: C{string}
        @param decompress: also decompress the file as this type
            (currently only '.gz' and '.bz2' are supported)
        @type decFile: C{twisted.python.FilePath}
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

class CacheManager:
    """Manages all requests for cached objects."""
    
    def __init__(self, cache_dir, db, other_dirs = [], manager = None):
        self.cache_dir = cache_dir
        self.other_dirs = other_dirs
        self.all_dirs = self.other_dirs[:]
        self.all_dirs.insert(0, self.cache_dir)
        self.db = db
        self.manager = manager
        self.scanning = []
        
        # Init the database, remove old files, init the HTTP dirs
        self.db.removeUntrackedFiles(self.all_dirs)
        self.db.reconcileDirectories()
        self.manager.setDirectories(self.db.getAllDirectories())
        
        
    def scanDirectories(self):
        """Scan the cache directories, hashing new and rehashing changed files."""
        assert not self.scanning, "a directory scan is already under way"
        self.scanning = self.all_dirs[:]
        self._scanDirectories()

    def _scanDirectories(self, result = None, walker = None):
        # Need to start waling a new directory
        if walker is None:
            # If there are any left, get them
            if self.scanning:
                log.msg('started scanning directory: %s' % self.scanning[0].path)
                walker = self.scanning[0].walk()
            else:
                # Done, just check if the HTTP directories need updating
                log.msg('cache directory scan complete')
                if self.db.reconcileDirectories():
                    self.manager.setDirectories(self.db.getAllDirectories())
                return
            
        try:
            # Get the next file in the directory
            file = walker.next()
        except StopIteration:
            # No files left, go to the next directory
            log.msg('done scanning directory: %s' % self.scanning[0].path)
            self.scanning.pop(0)
            reactor.callLater(0, self._scanDirectories)
            return

        # If it's not a file, or it's already properly in the DB, ignore it
        if not file.isfile() or self.db.isUnchanged(file):
            if not file.isfile():
                log.msg('entering directory: %s' % file.path)
            else:
                log.msg('file is unchanged: %s' % file.path)
            reactor.callLater(0, self._scanDirectories, None, walker)
            return

        # Otherwise hash it
        log.msg('start hash checking file: %s' % file.path)
        hash = HashObject()
        df = hash.hashInThread(file)
        df.addBoth(self._doneHashing, file, walker)
        df.addErrback(log.err)
    
    def _doneHashing(self, result, file, walker):
    
        if isinstance(result, HashObject):
            log.msg('hash check of %s completed with hash: %s' % (file.path, result.hexdigest()))
            if self.scanning[0] == self.cache_dir:
                mirror_dir = self.cache_dir.child(file.path[len(self.cache_dir.path)+1:].split('/', 1)[0])
                urlpath, newdir = self.db.storeFile(file, result.digest(), mirror_dir)
                url = 'http:/' + file.path[len(self.cache_dir.path):]
            else:
                urlpath, newdir = self.db.storeFile(file, result.digest(), self.scanning[0])
                url = None
            if newdir:
                self.manager.setDirectories(self.db.getAllDirectories())
            df = self.manager.new_cached_file(file, result, urlpath, url)
            if df is None:
                reactor.callLater(0, self._scanDirectories, None, walker)
            else:
                df.addBoth(self._scanDirectories, walker)
        else:
            log.msg('hash check of %s failed' % file.path)
            log.err(result)
            reactor.callLater(0, self._scanDirectories, None, walker)

    def save_file(self, response, hash, url):
        """Save a downloaded file to the cache and stream it."""
        if response.code != 200:
            log.msg('File was not found (%r): %s' % (response, url))
            return response
        
        log.msg('Returning file: %s' % url)
        
        parsed = urlparse(url)
        destFile = self.cache_dir.preauthChild(parsed[1] + parsed[2])
        log.msg('Saving returned %r byte file to cache: %s' % (response.stream.length, destFile.path))
        
        if destFile.exists():
            log.msg('File already exists, removing: %s' % destFile.path)
            destFile.remove()
        elif not destFile.parent().exists():
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
        response.stream.doneDefer.addCallback(self._save_complete, url, destFile,
                                              response.headers.getHeader('Last-Modified'),
                                              ext, decFile)
        response.stream.doneDefer.addErrback(self.save_error, url)
        return response

    def _save_complete(self, hash, url, destFile, modtime = None, ext = None, decFile = None):
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
                
            mirror_dir = self.cache_dir.child(destFile.path[len(self.cache_dir.path)+1:].split('/', 1)[0])
            urlpath, newdir = self.db.storeFile(destFile, hash.digest(), mirror_dir)
            log.msg('now avaliable at %s: %s' % (urlpath, url))

            if self.manager:
                if newdir:
                    log.msg('A new web directory was created, so enable it')
                    self.manager.setDirectories(self.db.getAllDirectories())
    
                self.manager.new_cached_file(destFile, hash, urlpath, url)
                if ext:
                    self.manager.new_cached_file(decFile, None, urlpath, url[:-len(ext)])
        else:
            log.msg("Hashes don't match %s != %s: %s" % (hash.hexexpected(), hash.hexdigest(), url))
            destFile.remove()
            if ext:
                decFile.remove()

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
        self.client = CacheManager(FilePath('/tmp/.apt-dht'))
        
    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.client = None
        