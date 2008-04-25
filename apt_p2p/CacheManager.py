
"""Manage a cache of downloaded files.

@var DECOMPRESS_EXTS: a list of file extensions that need to be decompressed
@var DECOMPRESS_FILES: a list of file names that need to be decompressed
"""

from urlparse import urlparse
import os

from twisted.python import log
from twisted.python.filepath import FilePath
from twisted.internet import defer, reactor
from twisted.trial import unittest
from twisted.web2.http import splitHostPort

from Streams import GrowingFileStream, StreamToFile
from Hash import HashObject
from apt_p2p_conf import config

DECOMPRESS_EXTS = ['.gz', '.bz2']
DECOMPRESS_FILES = ['release', 'sources', 'packages']

class CacheError(Exception):
    """Error occurred downloading a file to the cache."""

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
    
    def __init__(self, cache_dir, db, manager = None):
        """Initialize the instance and remove any untracked files from the DB..
        
        @type cache_dir: L{twisted.python.filepath.FilePath}
        @param cache_dir: the directory to use for storing all files
        @type db: L{db.DB}
        @param db: the database to use for tracking files and hashes
        @type manager: L{apt_p2p.AptP2P}
        @param manager: the main program object to send requests to
            (optional, defaults to not calling back with cached files)
        """
        self.cache_dir = cache_dir
        self.other_dirs = [FilePath(f) for f in config.getstringlist('DEFAULT', 'OTHER_DIRS')]
        self.all_dirs = self.other_dirs[:]
        self.all_dirs.insert(0, self.cache_dir)
        self.db = db
        self.manager = manager
        self.scanning = []
        
        # Init the database, remove old files
        self.db.removeUntrackedFiles(self.all_dirs)
        
    #{ Scanning directories
    def scanDirectories(self, result = None):
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
            reactor.callLater(0, self._scanDirectories, None, walker)
            return

        # If it's already properly in the DB, ignore it
        db_status = self.db.isUnchanged(file)
        if db_status:
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
    
    def _doneHashing(self, result, file, walker):
        """If successful, add the hashed file to the DB and inform the main program."""
        if isinstance(result, HashObject):
            log.msg('hash check of %s completed with hash: %s' % (file.path, result.hexdigest()))
            
            # Only set a URL if this is a downloaded file
            url = None
            if self.scanning[0] == self.cache_dir:
                url = 'http:/' + file.path[len(self.cache_dir.path):]
                
            # Store the hashed file in the database
            new_hash = self.db.storeFile(file, result.digest(), True,
                                         ''.join(result.pieceDigests()))
            
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
        if not destFile.parent().exists():
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
        f = destFile.open('w+')
        new_stream = GrowingFileStream(f, orig_stream.length)
        hash.new()
        df = StreamToFile(hash, orig_stream, f, notify = new_stream.updateAvailable,
                          decompress = ext, decFile = decFile).run()
        df.addCallback(self._save_complete, url, destFile, new_stream,
                       response.headers.getHeader('Last-Modified'), decFile)
        df.addErrback(self._save_error, url, destFile, new_stream, decFile)
        response.stream = new_stream

        # Return the modified response with the new stream
        return response

    def _save_complete(self, hash, url, destFile, destStream = None,
                       modtime = None, decFile = None):
        """Update the modification time and inform the main program.
        
        @type hash: L{Hash.HashObject}
        @param hash: the hash object containing the expected hash for the file
        @param url: the URI of the actual mirror request
        @type destFile: C{twisted.python.FilePath}
        @param destFile: the file where the download was written to
        @type destStream: L{Streams.GrowingFileStream}
        @param destStream: the stream to notify that all data is available
        @type modtime: C{int}
        @param modtime: the modified time of the cached file (seconds since epoch)
            (optional, defaults to not setting the modification time of the file)
        @type decFile: C{twisted.python.FilePath}
        @param decFile: the file where the decompressed download was written to
            (optional, defaults to the file not having been compressed)
        """
        result = hash.verify()
        if result or result is None:
            if destStream:
                destStream.allAvailable()
            if modtime:
                os.utime(destFile.path, (modtime, modtime))
            
            if result:
                log.msg('Hashes match: %s' % url)
                dht = True
            else:
                log.msg('Hashed file to %s: %s' % (hash.hexdigest(), url))
                dht = False
                
            new_hash = self.db.storeFile(destFile, hash.digest(), dht,
                                         ''.join(hash.pieceDigests()))

            if self.manager:
                self.manager.new_cached_file(destFile, hash, new_hash, url)

            if decFile:
                # Hash the decompressed file and add it to the DB
                decHash = HashObject()
                ext_len = len(destFile.path) - len(decFile.path)
                df = decHash.hashInThread(decFile)
                df.addCallback(self._save_complete, url[:-ext_len], decFile, modtime = modtime)
                df.addErrback(self._save_error, url[:-ext_len], decFile)
        else:
            log.msg("Hashes don't match %s != %s: %s" % (hash.hexexpected(), hash.hexdigest(), url))
            if destStream:
                destStream.allAvailable(remove = True)
            if decFile:
                decFile.remove()

    def _save_error(self, failure, url, destFile, destStream = None, decFile = None):
        """Remove the destination files."""
        log.msg('Error occurred downloading %s' % url)
        log.err(failure)
        if destStream:
            destStream.allAvailable(remove = True)
        else:
            destFile.restat(False)
            if destFile.exists():
                log.msg('Removing the incomplete file: %s' % destFile.path)
                destFile.remove()
        if decFile:
            decFile.restat(False)
            if decFile.exists():
                log.msg('Removing the incomplete file: %s' % decFile.path)
                decFile.remove()

    def save_error(self, failure, url):
        """An error has occurred in downloading or saving the file"""
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
        