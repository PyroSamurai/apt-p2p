# -*- test-case-name: twisted.test.test_policies -*-
# Copyright (c) 2001-2004 Twisted Matrix Laboratories.
# See LICENSE for details.

#

"""Resource limiting policies.

@seealso: See also L{twisted.protocols.htb} for rate limiting.
"""

# system imports
import sys, operator

# twisted imports
from twisted.internet.protocol import ServerFactory, Protocol, ClientFactory
from twisted.internet.interfaces import ITransport
from twisted.internet import reactor, error
from twisted.python import log
from zope.interface import implements, providedBy, directlyProvides

class ProtocolWrapper(Protocol):
    """Wraps protocol instances and acts as their transport as well."""

    disconnecting = 0

    def __init__(self, factory, wrappedProtocol):
        self.wrappedProtocol = wrappedProtocol
        self.factory = factory

    def makeConnection(self, transport):
        directlyProvides(self, *providedBy(self) + providedBy(transport))
        Protocol.makeConnection(self, transport)

    # Transport relaying

    def write(self, data):
        self.transport.write(data)

    def writeSequence(self, data):
        self.transport.writeSequence(data)

    def loseConnection(self):
        self.disconnecting = 1
        self.transport.loseConnection()

    def getPeer(self):
        return self.transport.getPeer()

    def getHost(self):
        return self.transport.getHost()

    def registerProducer(self, producer, streaming):
        self.transport.registerProducer(producer, streaming)

    def unregisterProducer(self):
        self.transport.unregisterProducer()

    def stopConsuming(self):
        self.transport.stopConsuming()

    def __getattr__(self, name):
        return getattr(self.transport, name)

    # Protocol relaying

    def connectionMade(self):
        self.factory.registerProtocol(self)
        self.wrappedProtocol.makeConnection(self)

    def dataReceived(self, data):
        self.wrappedProtocol.dataReceived(data)

    def connectionLost(self, reason):
        self.factory.unregisterProtocol(self)
        self.wrappedProtocol.connectionLost(reason)


class WrappingFactory(ClientFactory):
    """Wraps a factory and its protocols, and keeps track of them."""

    protocol = ProtocolWrapper

    def __init__(self, wrappedFactory):
        self.wrappedFactory = wrappedFactory
        self.protocols = {}

    def doStart(self):
        self.wrappedFactory.doStart()
        ClientFactory.doStart(self)

    def doStop(self):
        self.wrappedFactory.doStop()
        ClientFactory.doStop(self)

    def startedConnecting(self, connector):
        self.wrappedFactory.startedConnecting(connector)

    def clientConnectionFailed(self, connector, reason):
        self.wrappedFactory.clientConnectionFailed(connector, reason)

    def clientConnectionLost(self, connector, reason):
        self.wrappedFactory.clientConnectionLost(connector, reason)

    def buildProtocol(self, addr):
        return self.protocol(self, self.wrappedFactory.buildProtocol(addr))

    def registerProtocol(self, p):
        """Called by protocol to register itself."""
        self.protocols[p] = 1

    def unregisterProtocol(self, p):
        """Called by protocols when they go away."""
        del self.protocols[p]


class ThrottlingProtocol(ProtocolWrapper):
    """Protocol for ThrottlingFactory."""

    # wrap API for tracking bandwidth

    def __init__(self, factory, wrappedProtocol):
        ProtocolWrapper.__init__(self, factory, wrappedProtocol)
        self._tempDataBuffer = []
        self._tempDataLength = 0
        self.throttled = False

    def write(self, data):
        # Check if we can write
        if (not self.throttled) and self.factory.registerWritten(len(data)):
            ProtocolWrapper.write(self, data)
            
            if hasattr(self, "producer") and self.producer and not self.producer.paused:
                # Interrupt the flow so that others can can have a chance
                # We can only do this if it's not already paused otherwise we
                # risk unpausing something that the Server paused
                self.producer.pauseProducing()
                reactor.callLater(0, self.producer.resumeProducing)
        else:
            # Can't write, buffer the data
            self._tempDataBuffer.append(data)
            self._tempDataLength += len(data)
            self._throttleWrites()

    def writeSequence(self, seq):
        if not self.throttled:
            # Write each sequence separately
            while seq and self.factory.registerWritten(len(seq[0])):
                ProtocolWrapper.write(self, seq.pop(0))

        # If there's some left, we must have been throttled
        if seq:
            self._tempDataBuffer.extend(seq)
            self._tempDataLength += reduce(operator.add, map(len, seq))
            self._throttleWrites()

    def dataReceived(self, data):
        self.factory.registerRead(len(data))
        ProtocolWrapper.dataReceived(self, data)

    def registerProducer(self, producer, streaming):
        self.producer = producer
        ProtocolWrapper.registerProducer(self, producer, streaming)

    def unregisterProducer(self):
        del self.producer
        ProtocolWrapper.unregisterProducer(self)


    def throttleReads(self):
        self.transport.pauseProducing()

    def unthrottleReads(self):
        self.transport.resumeProducing()

    def _throttleWrites(self):
        # If we haven't yet, queue for unthrottling
        if not self.throttled:
            self.throttled = True
            self.factory.throttledWrites(self)

        if hasattr(self, "producer") and self.producer:
            self.producer.pauseProducing()

    def unthrottleWrites(self):
        # Write some data
        if self._tempDataBuffer:
            assert self.factory.registerWritten(len(self._tempDataBuffer[0]))
            self._tempDataLength -= len(self._tempDataBuffer[0])
            ProtocolWrapper.write(self, self._tempDataBuffer.pop(0))
            assert self._tempDataLength >= 0

        # If we wrote it all, start producing more
        if not self._tempDataBuffer:
            assert self._tempDataLength == 0
            self.throttled = False
            if hasattr(self, "producer") and self.producer:
                # This might unpause something the Server has also paused, but
                # it will get paused again on first write anyway
                reactor.callLater(0, self.producer.resumeProducing)
        
        return self._tempDataLength


class ThrottlingFactory(WrappingFactory):
    """Throttles bandwidth and number of connections.

    Write bandwidth will only be throttled if there is a producer
    registered.
    """

    protocol = ThrottlingProtocol
    CHUNK_SIZE = 4*1024

    def __init__(self, wrappedFactory, maxConnectionCount=sys.maxint, readLimit=None, writeLimit=None):
        WrappingFactory.__init__(self, wrappedFactory)
        self.connectionCount = 0
        self.maxConnectionCount = maxConnectionCount
        self.readLimit = readLimit # max bytes we should read per second
        self.writeLimit = writeLimit # max bytes we should write per second
        self.readThisSecond = 0
        self.writeAvailable = writeLimit
        self._writeQueue = []
        self.unthrottleReadsID = None
        self.checkReadBandwidthID = None
        self.unthrottleWritesID = None
        self.checkWriteBandwidthID = None

    def registerWritten(self, length):
        """Called by protocol to tell us more bytes were written."""
        # Check if there are bytes available to write
        if self.writeAvailable > 0:
            self.writeAvailable -= length
            return True
        
        return False
    
    def throttledWrites(self, p):
        """Called by the protocol to queue it for later writing."""
        assert p not in self._writeQueue
        self._writeQueue.append(p)

    def registerRead(self, length):
        """Called by protocol to tell us more bytes were read."""
        self.readThisSecond += length

    def checkReadBandwidth(self):
        """Checks if we've passed bandwidth limits."""
        if self.readThisSecond > self.readLimit:
            self.throttleReads()
            throttleTime = (float(self.readThisSecond) / self.readLimit) - 1.0
            self.unthrottleReadsID = reactor.callLater(throttleTime,
                                                       self.unthrottleReads)
        self.readThisSecond = 0
        self.checkReadBandwidthID = reactor.callLater(1, self.checkReadBandwidth)

    def checkWriteBandwidth(self):
        """Add some new available bandwidth, and check for protocols to unthrottle."""
        # Increase the available write bytes, but not higher than the limit
        self.writeAvailable = min(self.writeLimit, self.writeAvailable + self.writeLimit)
        
        # Write from the queue until it's empty or we're throttled again
        while self.writeAvailable > 0 and self._writeQueue:
            # Get the first queued protocol
            p = self._writeQueue.pop(0)
            _tempWriteAvailable = self.writeAvailable
            bytesLeft = 1
            
            # Unthrottle writes until CHUNK_SIZE is reached or the protocol is unbuffered
            while self.writeAvailable > 0 and _tempWriteAvailable - self.writeAvailable < self.CHUNK_SIZE and bytesLeft > 0:
                # Unthrottle a single write (from the protocol's buffer)
                bytesLeft = p.unthrottleWrites()
                
            # If the protocol is not done, requeue it
            if bytesLeft > 0:
                self._writeQueue.append(p)

        self.checkWriteBandwidthID = reactor.callLater(1, self.checkWriteBandwidth)

    def throttleReads(self):
        """Throttle reads on all protocols."""
        log.msg("Throttling reads on %s" % self)
        for p in self.protocols.keys():
            p.throttleReads()

    def unthrottleReads(self):
        """Stop throttling reads on all protocols."""
        self.unthrottleReadsID = None
        log.msg("Stopped throttling reads on %s" % self)
        for p in self.protocols.keys():
            p.unthrottleReads()

    def buildProtocol(self, addr):
        if self.connectionCount == 0:
            if self.readLimit is not None:
                self.checkReadBandwidth()
            if self.writeLimit is not None:
                self.checkWriteBandwidth()

        if self.connectionCount < self.maxConnectionCount:
            self.connectionCount += 1
            return WrappingFactory.buildProtocol(self, addr)
        else:
            log.msg("Max connection count reached!")
            return None

    def unregisterProtocol(self, p):
        WrappingFactory.unregisterProtocol(self, p)
        self.connectionCount -= 1
        if self.connectionCount == 0:
            if self.unthrottleReadsID is not None:
                self.unthrottleReadsID.cancel()
            if self.checkReadBandwidthID is not None:
                self.checkReadBandwidthID.cancel()
            if self.unthrottleWritesID is not None:
                self.unthrottleWritesID.cancel()
            if self.checkWriteBandwidthID is not None:
                self.checkWriteBandwidthID.cancel()

