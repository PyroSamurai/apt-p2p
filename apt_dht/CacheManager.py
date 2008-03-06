
"""Manage a cache of downloaded files.

@var DECOMPRESS_EXTS: a list of file extensions that need to be decompressed
@var DECOMPRESS_FILES: a list of file names that need to be decompressed
"""

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

DECOMPRESS_EXTS = ['.gz', '.bz2']
DECOMPRESS_FILES = ['release', 'sources', 'packages']

class ProxyFileStream(stream.SimpleStream):
    """Saves a stream to a file while providing a new stream.
    
    Also optionally decompresses the file while it is being downloaded.
    
    @type stream: L{twisted.web2.stream.IByteStream}
    @ivar stream: the input stream being read
    @type outFile: L{twisted.python.filepath.FilePath}
    @ivar outFile: the file being written
    @type hash: L{Hash.HashObject}
    @ivar hash: the hash object for the file
    @type gzfile: C{file}
    @ivar gzfile: the open file to write decompressed gzip data to
    @type gzdec: L{zlib.decompressobj}
    @ivar gzdec: the decompressor to use for the compressed gzip data
    @type gzheader: C{boolean}
    @ivar gzheader: whether the gzip header still needs to be removed from
        the zlib compressed data
    @type bz2file: C{file}
    @ivar bz2file: the open file to write decompressed bz2 data to
    @type bz2dec: L{bz2.BZ2Decompressor}
    @ivar bz2dec: the decompressor to use for the compressed bz2 data
    @type length: C{int}
    @ivar length: the length of the original (compressed) file
    @type doneDefer: L{twisted.internet.defer.Deferred}
    @ivar doneDefer: the deferred that will fire when done streaming
    
    @group Stream implementation: read, close
    
    """
    
    def __init__(self, stream, outFile, hash, decompress = None, decFile = None):
        """Initializes the proxy.
        
        @type stream: L{twisted.web2.stream.IByteStream}
        @param stream: the input stream to read from
        @type outFile: L{twisted.python.filepath.FilePath}
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
        self.doneDefer = defer.Deferred()

    def _done(self):
        """Close all the output files, return the result."""
        if not self.outFile.closed:
            self.outFile.close()
            self.hash.digest()
            if self.gzfile:
                # Finish the decompression
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
        
        # Read data from the stream, deal with the possible deferred
        data = self.stream.read()
        if isinstance(data, defer.Deferred):
            data.addCallbacks(self._write, self._done)
            return data
        
        self._write(data)
        return data
    
    def _write(self, data):
        """Write the stream data to the file and return it for others to use.
        
        Also optionally decompresses it.
        """
        if data is None:
            self._done()
            return data
        
        # Write and hash the streamed data
        self.outFile.write(data)
        self.hash.update(data)
        
        if self.gzfile:
            # Decompress the zlib portion of the file
            if self.gzheader:
                # Remove the gzip header junk
                self.gzheader = False
                new_data = self._remove_gzip_header(data)
                dec_data = self.gzdec.decompress(new_data)
            else:
                dec_data = self.gzdec.decompress(data)
            self.gzfile.write(dec_data)
        if self.bz2file:
            # Decompress the bz2 file
            dec_data = self.bz2dec.decompress(data)
            self.bz2file.write(dec_data)

        return data
    
    def _remove_gzip_header(self, data):
        """Remove the gzip header from the zlib compressed data."""
        # Read, check & discard the header fields
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
            # Read & discard the extra field
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
    """Manages all downloaded files and requests for cached objects.
    
    @type cache_dir: L{twisted.python.filepath.FilePath}
    @ivar cache_dir: the directory to use for storing all files
    @type other_dirs: C{list} of L{twisted.python.filepath.FilePath}
    @ivar other_dirs: the other directories that have shared files in them
    @type all_dirs: C{list} of L{twisted.python.filepath.FilePath}
    @ivar all_dirs: all the directories that have cached files in them
    @type db: L{db.DB}
    @ivar db: the database to use for tracking files and hashes
    @type manager: L{apt_p2p.AptP2P}
    @ivar manager: the main program object to send requests to
    @type scanning: C{list} of L{twisted.python.filepath.FilePath}
    @ivar scanning: all the directories that are currectly being scanned or waiting to be scanned
    """
    
    def __init__(self, cache_dir, db, other_dirs = [], manager = None):
        """Initialize the instance and remove any untracked files from the DB..
        
        @type cache_dir: L{twisted.python.filepath.FilePath}
        @param cache_dir: the directory to use for storing all files
        @type db: L{db.DB}
        @param db: the database to use for tracking files and hashes
        @type other_dirs: C{list} of L{twisted.python.filepath.FilePath}
        @param other_dirs: the other directories that have shared files in them
            (optional, defaults to only using the cache directory)
        @type manager: L{apt_p2p.AptP2P}
        @param manager: the main program object to send requests to
            (optional, defaults to not calling back with cached files)
        """
        self.cache_dir = cache_dir
        self.other_dirs = other_dirs
        self.all_dirs = self.other_dirs[:]
        self.all_dirs.insert(0, self.cache_dir)
        self.db = db
        self.manager = manager
        self.scanning = []
        
        # Init the database, remove old files
        self.db.removeUntrackedFiles(self.all_dirs)
        
    #{ Scanning directories
    def scanDirectories(self):
        """Scan the cache directories, hashing new and rehashing changed files."""
        assert not self.scanning, "a directory scan is already under way"
        self.scanning = self.all_dirs[:]
        self._scanDirectories()

    def _scanDirectories(self, result = None, walker = None):
        """Walk each directory looking for cached files.
        
        @param result: the result of a DHT store request, not used (optional)
        @param walker: the walker to use to traverse the current directory
            (optional, defaults to creating a new walker from the first
            directory in the L{CacheManager.scanning} list)
        """
        # Need to start walking a new directory
        if walker is None:
            # If there are any left, get them
            if self.scanning:
                log.msg('started scanning directory: %s' % self.scanning[0].path)
                walker = self.scanning[0].walk()
            else:
                log.msg('cache directory scan complete')
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

        # If it's not a file ignore it
        if not file.isfile():
            log.msg('entering directory: %s' % file.path)
            reactor.callLater(0, self._scanDirectories, None, walker)
            return

        # If it's already properly in the DB, ignore it
        db_status = self.db.isUnchanged(file)
        if db_status:
            log.msg('file is unchanged: %s' % file.path)
            reactor.callLater(0, self._scanDirectories, None, walker)
            return
        
        # Don't hash files in the cache that are not in the DB
        if self.scanning[0] == self.cache_dir:
            if db_status is None:
                log.msg('ignoring unknown cache file: %s' % file.path)
            else:
                log.msg('removing changed cache file: %s' % file.path)
                file.remove()
            reactor.callLater(0, self._scanDirectories, None, walker)
            return

        # Otherwise hash it
        log.msg('start hash checking file: %s' % file.path)
        hash = HashObject()
        df = hash.hashInThread(file)
        df.addBoth(self._doneHashing, file, walker)
        df.addErrback(log.err)
    
    def _doneHashing(self, result, file, walker):
        """If successful, add the hashed file to the DB and inform the main program."""
        if isinstance(result, HashObject):
            log.msg('hash check of %s completed with hash: %s' % (file.path, result.hexdigest()))
            
            # Only set a URL if this is a downloaded file
            url = None
            if self.scanning[0] == self.cache_dir:
                url = 'http:/' + file.path[len(self.cache_dir.path):]
                
            # Store the hashed file in the database
            new_hash = self.db.storeFile(file, result.digest())
            
            # Tell the main program to handle the new cache file
            df = self.manager.new_cached_file(file, result, new_hash, url, True)
            if df is None:
                reactor.callLater(0, self._scanDirectories, None, walker)
            else:
                df.addBoth(self._scanDirectories, walker)
        else:
            # Must have returned an error
            log.msg('hash check of %s failed' % file.path)
            log.err(result)
            reactor.callLater(0, self._scanDirectories, None, walker)

    #{ Downloading files
    def save_file(self, response, hash, url):
        """Save a downloaded file to the cache and stream it.
        
        @type response: L{twisted.web2.http.Response}
        @param response: the response from the download
        @type hash: L{Hash.HashObject}
        @param hash: the hash object containing the expected hash for the file
        @param url: the URI of the actual mirror request
        @rtype: L{twisted.web2.http.Response}
        @return: the final response from the download
        """
        if response.code != 200:
            log.msg('File was not found (%r): %s' % (response, url))
            return response
        
        log.msg('Returning file: %s' % url)

        # Set the destination path for the file
        parsed = urlparse(url)
        destFile = self.cache_dir.preauthChild(parsed[1] + parsed[2])
        log.msg('Saving returned %r byte file to cache: %s' % (response.stream.length, destFile.path))
        
        # Make sure there's a free place for the file
        if destFile.exists():
            log.msg('File already exists, removing: %s' % destFile.path)
            destFile.remove()
        elif not destFile.parent().exists():
            destFile.parent().makedirs()

        # Determine whether it needs to be decompressed and how
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
            
        # Create the new stream from the old one.
        orig_stream = response.stream
        response.stream = ProxyFileStream(orig_stream, destFile, hash, ext, decFile)
        response.stream.doneDefer.addCallback(self._save_complete, url, destFile,
                                              response.headers.getHeader('Last-Modified'),
                                              decFile)
        response.stream.doneDefer.addErrback(self.save_error, url)

        # Return the modified response with the new stream
        return response

    def _save_complete(self, hash, url, destFile, modtime = None, decFile = None):
        """Update the modification time and inform the main program.
        
        @type hash: L{Hash.HashObject}
        @param hash: the hash object containing the expected hash for the file
        @param url: the URI of the actual mirror request
        @type destFile: C{twisted.python.FilePath}
        @param destFile: the file where the download was written to
        @type modtime: C{int}
        @param modtime: the modified time of the cached file (seconds since epoch)
            (optional, defaults to not setting the modification time of the file)
        @type decFile: C{twisted.python.FilePath}
        @param decFile: the file where the decompressed download was written to
            (optional, defaults to the file not having been compressed)
        """
        if modtime:
            os.utime(destFile.path, (modtime, modtime))
            if decFile:
                os.utime(decFile.path, (modtime, modtime))
        
        result = hash.verify()
        if result or result is None:
            if result:
                log.msg('Hashes match: %s' % url)
            else:
                log.msg('Hashed file to %s: %s' % (hash.hexdigest(), url))
                
            new_hash = self.db.storeFile(destFile, hash.digest())
            log.msg('now avaliable: %s' % (url))

            if self.manager:
                self.manager.new_cached_file(destFile, hash, new_hash, url)
                if decFile:
                    ext_len = len(destFile.path) - len(decFile.path)
                    self.manager.new_cached_file(decFile, None, False, url[:-ext_len])
        else:
            log.msg("Hashes don't match %s != %s: %s" % (hash.hexexpected(), hash.hexdigest(), url))
            destFile.remove()
            if decFile:
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
        self.client = CacheManager(FilePath('/tmp/.apt-p2p'))
        
    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.client = None
        