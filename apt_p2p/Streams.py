
"""Modified streams that are used by Apt-P2P."""

from bz2 import BZ2Decompressor
from zlib import decompressobj, MAX_WBITS
from gzip import FCOMMENT, FEXTRA, FHCRC, FNAME, FTEXT
import os

from twisted.web2 import stream
from twisted.internet import defer
from twisted.python import log, filepath

class StreamsError(Exception):
    """An error occurred in the streaming."""

class GrowingFileStream(stream.SimpleStream):
    """Modified to stream data from a file as it becomes available.
    
    @ivar CHUNK_SIZE: the maximum size of chunks of data to send at a time
    @ivar deferred: waiting for the result of the last read attempt
    @ivar available: the number of bytes that are currently available to read
    @ivar position: the current position in the file where the next read will begin
    @ivar closed: True if the reader has closed the stream
    @ivar finished: True when no more data will be coming available
    @ivar remove: whether to remove the file when streaming is complete
    """

    CHUNK_SIZE = 32*1024

    def __init__(self, f, length = None):
        self.f = f
        self.length = length
        self.deferred = None
        self.available = 0L
        self.position = 0L
        self.closed = False
        self.finished = False
        self.remove = False

    #{ Stream interface
    def read(self, sendfile=False):
        assert not self.deferred, "A previous read is still deferred."

        if self.f is None:
            return None

        length = self.available - self.position
        readSize = min(length, self.CHUNK_SIZE)

        # If we don't have any available, we're done or deferred
        if readSize <= 0:
            if self.finished:
                self._close()
                return None
            else:
                self.deferred = defer.Deferred()
                return self.deferred

        # Try to read some data from the file
        self.f.seek(self.position)
        b = self.f.read(readSize)
        bytesRead = len(b)
        if not bytesRead:
            # End of file was reached, we're done or deferred
            if self.finished:
                self._close()
                return None
            else:
                self.deferred = defer.Deferred()
                return self.deferred
        else:
            self.position += bytesRead
            return b
    
    def split(self, point):
        raise StreamsError, "You can not split a GrowingFileStream"
    
    def close(self):
        self.length = 0
        self.closed = True
        self._close()

    #{ Growing functions
    def updateAvailable(self, newlyAvailable):
        """Update the number of bytes that are available.
        
        Call it with 0 to trigger reading of a fully read file.
        
        @param newlyAvailable: the number of bytes that just became available
        """
        if not self.finished:
            self.available += newlyAvailable
        
        # If a read is pending, let it go
        if self.deferred and self.position < self.available:
            # Try to read some data from the file
            length = self.available - self.position
            readSize = min(length, self.CHUNK_SIZE)
            self.f.seek(self.position)
            b = self.f.read(readSize)
            bytesRead = len(b)
            
            # Check if end of file was reached
            if bytesRead:
                self.position += bytesRead
                deferred = self.deferred
                self.deferred = None
                deferred.callback(b)

    def allAvailable(self, remove = False):
        """Indicate that no more data will be coming available.
        
        @param remove: whether to remove the file when streaming is complete
        """
        self.finished = True
        self.remove = remove

        # If a read is pending, let it go
        if self.deferred:
            if self.position < self.available:
                # Try to read some data from the file
                length = self.available - self.position
                readSize = min(length, self.CHUNK_SIZE)
                self.f.seek(self.position)
                b = self.f.read(readSize)
                bytesRead = len(b)
    
                # Check if end of file was reached
                if bytesRead:
                    self.position += bytesRead
                    deferred = self.deferred
                    self.deferred = None
                    deferred.callback(b)
                else:
                    # We're done
                    self._close()
                    deferred = self.deferred
                    self.deferred = None
                    deferred.callback(None)
            else:
                # We're done
                self._close()
                deferred = self.deferred
                self.deferred = None
                deferred.callback(None)
                
        if self.closed:
            self._close()
        
    def _close(self):
        """Close the temporary file and maybe remove it."""
        if self.f:
            self.f.close()
            if self.remove:
                file = filepath.FilePath(self.f.name)
                file.restat(False)
                if file.exists():
                    file.remove()
            self.f = None
        
class StreamToFile:
    """Save a stream to a partial file and hash it.
    
    Also optionally decompresses the file while it is being downloaded.

    @type stream: L{twisted.web2.stream.IByteStream}
    @ivar stream: the input stream being read
    @type outFile: C{file}
    @ivar outFile: the open file being written
    @type hasher: hashing object, e.g. C{sha1}
    @ivar hasher: the hash object for the data
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
    @type position: C{int}
    @ivar position: the current file position to write the next data to
    @type length: C{int}
    @ivar length: the position in the file to not write beyond
    @ivar notify: a method that will be notified of the length of received data
    @type doneDefer: L{twisted.internet.defer.Deferred}
    @ivar doneDefer: the deferred that will fire when done writing
    """
    
    def __init__(self, hasher, inputStream, outFile, start = 0, length = None,
                 notify = None, decompress = None, decFile = None):
        """Initializes the files.
        
        @type hasher: hashing object, e.g. C{sha1}
        @param hasher: the hash object for the data
        @type inputStream: L{twisted.web2.stream.IByteStream}
        @param inputStream: the input stream to read from
        @type outFile: C{file}
        @param outFile: the open file to write to
        @type start: C{int}
        @param start: the file position to start writing at
            (optional, defaults to the start of the file)
        @type length: C{int}
        @param length: the maximum amount of data to write to the file
            (optional, defaults to not limiting the writing to the file
        @param notify: a method that will be notified of the length of
            received data (optional)
        @type decompress: C{string}
        @param decompress: also decompress the file as this type
            (currently only '.gz' and '.bz2' are supported)
        @type decFile: C{twisted.python.FilePath}
        @param decFile: the file to write the decompressed data to
        """
        self.stream = inputStream
        self.outFile = outFile
        self.hasher = hasher
        self.gzfile = None
        self.bz2file = None
        if decompress == ".gz":
            self.gzheader = True
            self.gzfile = decFile.open('w')
            self.gzdec = decompressobj(-MAX_WBITS)
        elif decompress == ".bz2":
            self.bz2file = decFile.open('w')
            self.bz2dec = BZ2Decompressor()
        self.position = start
        self.length = None
        if length is not None:
            self.length = start + length
        self.notify = notify
        self.doneDefer = None
        
    def run(self):
        """Start the streaming.

        @rtype: L{twisted.internet.defer.Deferred}
        """
        self.doneDefer = stream.readStream(self.stream, self._gotData)
        self.doneDefer.addCallbacks(self._done, self._error)
        return self.doneDefer

    def _gotData(self, data):
        """Process the received data."""
        if self.outFile.closed:
            raise StreamsError, "outFile was unexpectedly closed"
        
        # Make sure we don't go too far
        if self.length is not None and self.position + len(data) > self.length:
            data = data[:(self.length - self.position)]
        
        # Write and hash the streamed data
        self.outFile.seek(self.position)
        self.outFile.write(data)
        self.hasher.update(data)
        self.position += len(data)
        
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
            
        if self.notify:
            self.notify(len(data))

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

    def _close(self):
        """Close all the output files."""
        # Can't close the outfile, but we should sync it to disk
        if not self.outFile.closed:
            self.outFile.flush()
        
        # Close the decompressed file
        if self.gzfile:
            # Finish the decompression 
            data_dec = self.gzdec.flush()
            self.gzfile.write(data_dec)
            self.gzfile.close()
            self.gzfile = None
        if self.bz2file:
            self.bz2file.close()
            self.bz2file = None
    
    def _done(self, result):
        """Return the result."""
        self._close()
        return self.hasher
    
    def _error(self, err):
        """Log the error and close everything."""
        log.msg('Streaming error')
        log.err(err)
        self.stream.close()
        self._close()
        return err
    
class UploadStream:
    """Identifier for streams that are uploaded to peers."""
    
class PiecesUploadStream(stream.MemoryStream, UploadStream):
    """Modified to identify it for streaming to peers."""

class FileUploadStream(stream.FileStream, UploadStream):
    """Modified to make it suitable for streaming to peers.
    
    Streams the file in small chunks to make it easier to throttle the
    streaming to peers.
    
    @ivar CHUNK_SIZE: the size of chunks of data to send at a time
    """

    CHUNK_SIZE = 4*1024
    
    def read(self, sendfile=False):
        if self.f is None:
            return None

        length = self.length
        if length == 0:
            self.f = None
            return None
        
        # Remove the SendFileBuffer and mmap use, just use string reads and writes

        readSize = min(length, self.CHUNK_SIZE)

        self.f.seek(self.start)
        b = self.f.read(readSize)
        bytesRead = len(b)
        if not bytesRead:
            raise RuntimeError("Ran out of data reading file %r, expected %d more bytes" % (self.f, length))
        else:
            self.length -= bytesRead
            self.start += bytesRead
            return b
