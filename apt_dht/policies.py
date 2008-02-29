# -*- test-case-name: twisted.test.test_policies -*-
# Copyright (c) 2001-2007 Twisted Matrix Laboratories.
# See LICENSE for details.


"""
Resource limiting policies.

@seealso: See also L{twisted.protocols.htb} for rate limiting.
"""

# system imports
import sys, operator

# twisted imports
from twisted.internet.protocol import ServerFactory, Protocol, ClientFactory
from twisted.internet import reactor, error
from twisted.python import log
from zope.interface import providedBy, directlyProvides


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
        if not self.throttled:
            paused = self.factory.registerWritten(len(data))
            if not paused:
                ProtocolWrapper.write(self, data)
                
                if paused is not None and hasattr(self, "producer") and self.producer and not self.producer.paused:
                    # Interrupt the flow so that others can can have a chance
                    # We can only do this if it's not already paused otherwise we
                    # risk unpausing something that the Server paused
                    self.producer.pauseProducing()
                    reactor.callLater(0, self.producer.resumeProducing)

        if self.throttled or paused:
            # Can't write, buffer the data
            self._tempDataBuffer.append(data)
            self._tempDataLength += len(data)
            self._throttleWrites()

    def writeSequence(self, seq):
        if not self.throttled:
            # Write each sequence separately
            while seq and not self.factory.registerWritten(len(seq[0])):
                ProtocolWrapper.write(self, seq.pop(0))

        # If there's some left, we must have been paused
        if seq:
            self._tempDataBuffer.extend(seq)
            self._tempDataLength += reduce(operator.add, map(len, seq))
            self._throttleWrites()

    def dataReceived(self, data):
        self.factory.registerRead(len(data))
        ProtocolWrapper.dataReceived(self, data)

    def registerProducer(self, producer, streaming):
        assert streaming, "You can only use the ThrottlingProtocol with streaming (push) producers."
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
            assert not self.factory.registerWritten(len(self._tempDataBuffer[0]))
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
    """
    Throttles bandwidth and number of connections.

    Write bandwidth will only be throttled if there is a producer
    registered.
    """

    protocol = ThrottlingProtocol
    CHUNK_SIZE = 4*1024

    def __init__(self, wrappedFactory, maxConnectionCount=sys.maxint,
                 readLimit=None, writeLimit=None):
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


    def callLater(self, period, func):
        """
        Wrapper around L{reactor.callLater} for test purpose.
        """
        return reactor.callLater(period, func)


    def registerWritten(self, length):
        """
        Called by protocol to tell us more bytes were written.
        Returns True if the bytes could not be written and the protocol should pause itself.
        """
        # Check if there are bytes available to write
        if self.writeLimit is None:
            return None
        elif self.writeAvailable > 0:
            self.writeAvailable -= length
            return False
        
        return True

    
    def throttledWrites(self, p):
        """
        Called by the protocol to queue it for later writing.
        """
        assert p not in self._writeQueue
        self._writeQueue.append(p)


    def registerRead(self, length):
        """
        Called by protocol to tell us more bytes were read.
        """
        self.readThisSecond += length


    def checkReadBandwidth(self):
        """
        Checks if we've passed bandwidth limits.
        """
        if self.readThisSecond > self.readLimit:
            self.throttleReads()
            throttleTime = (float(self.readThisSecond) / self.readLimit) - 1.0
            self.unthrottleReadsID = self.callLater(throttleTime,
                                                    self.unthrottleReads)
        self.readThisSecond = 0
        self.checkReadBandwidthID = self.callLater(1, self.checkReadBandwidth)


    def checkWriteBandwidth(self):
        """
        Add some new available bandwidth, and check for protocols to unthrottle.
        """
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

        self.checkWriteBandwidthID = self.callLater(1, self.checkWriteBandwidth)


    def throttleReads(self):
        """
        Throttle reads on all protocols.
        """
        log.msg("Throttling reads on %s" % self)
        for p in self.protocols.keys():
            p.throttleReads()


    def unthrottleReads(self):
        """
        Stop throttling reads on all protocols.
        """
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



class SpewingProtocol(ProtocolWrapper):
    def dataReceived(self, data):
        log.msg("Received: %r" % data)
        ProtocolWrapper.dataReceived(self,data)

    def write(self, data):
        log.msg("Sending: %r" % data)
        ProtocolWrapper.write(self,data)



class SpewingFactory(WrappingFactory):
    protocol = SpewingProtocol



class LimitConnectionsByPeer(WrappingFactory):
    """Stability: Unstable"""

    maxConnectionsPerPeer = 5

    def startFactory(self):
        self.peerConnections = {}

    def buildProtocol(self, addr):
        peerHost = addr[0]
        connectionCount = self.peerConnections.get(peerHost, 0)
        if connectionCount >= self.maxConnectionsPerPeer:
            return None
        self.peerConnections[peerHost] = connectionCount + 1
        return WrappingFactory.buildProtocol(self, addr)

    def unregisterProtocol(self, p):
        peerHost = p.getPeer()[1]
        self.peerConnections[peerHost] -= 1
        if self.peerConnections[peerHost] == 0:
            del self.peerConnections[peerHost]


class LimitTotalConnectionsFactory(ServerFactory):
    """Factory that limits the number of simultaneous connections.

    API Stability: Unstable

    @type connectionCount: C{int}
    @ivar connectionCount: number of current connections.
    @type connectionLimit: C{int} or C{None}
    @cvar connectionLimit: maximum number of connections.
    @type overflowProtocol: L{Protocol} or C{None}
    @cvar overflowProtocol: Protocol to use for new connections when
        connectionLimit is exceeded.  If C{None} (the default value), excess
        connections will be closed immediately.
    """
    connectionCount = 0
    connectionLimit = None
    overflowProtocol = None

    def buildProtocol(self, addr):
        if (self.connectionLimit is None or
            self.connectionCount < self.connectionLimit):
                # Build the normal protocol
                wrappedProtocol = self.protocol()
        elif self.overflowProtocol is None:
            # Just drop the connection
            return None
        else:
            # Too many connections, so build the overflow protocol
            wrappedProtocol = self.overflowProtocol()

        wrappedProtocol.factory = self
        protocol = ProtocolWrapper(self, wrappedProtocol)
        self.connectionCount += 1
        return protocol

    def registerProtocol(self, p):
        pass

    def unregisterProtocol(self, p):
        self.connectionCount -= 1



class TimeoutProtocol(ProtocolWrapper):
    """
    Protocol that automatically disconnects when the connection is idle.

    Stability: Unstable
    """

    def __init__(self, factory, wrappedProtocol, timeoutPeriod):
        """
        Constructor.

        @param factory: An L{IFactory}.
        @param wrappedProtocol: A L{Protocol} to wrapp.
        @param timeoutPeriod: Number of seconds to wait for activity before
            timing out.
        """
        ProtocolWrapper.__init__(self, factory, wrappedProtocol)
        self.timeoutCall = None
        self.setTimeout(timeoutPeriod)


    def setTimeout(self, timeoutPeriod=None):
        """
        Set a timeout.

        This will cancel any existing timeouts.

        @param timeoutPeriod: If not C{None}, change the timeout period.
            Otherwise, use the existing value.
        """
        self.cancelTimeout()
        if timeoutPeriod is not None:
            self.timeoutPeriod = timeoutPeriod
        self.timeoutCall = self.factory.callLater(self.timeoutPeriod, self.timeoutFunc)


    def cancelTimeout(self):
        """
        Cancel the timeout.

        If the timeout was already cancelled, this does nothing.
        """
        if self.timeoutCall:
            try:
                self.timeoutCall.cancel()
            except error.AlreadyCalled:
                pass
            self.timeoutCall = None


    def resetTimeout(self):
        """
        Reset the timeout, usually because some activity just happened.
        """
        if self.timeoutCall:
            self.timeoutCall.reset(self.timeoutPeriod)


    def write(self, data):
        self.resetTimeout()
        ProtocolWrapper.write(self, data)


    def writeSequence(self, seq):
        self.resetTimeout()
        ProtocolWrapper.writeSequence(self, seq)


    def dataReceived(self, data):
        self.resetTimeout()
        ProtocolWrapper.dataReceived(self, data)


    def connectionLost(self, reason):
        self.cancelTimeout()
        ProtocolWrapper.connectionLost(self, reason)


    def timeoutFunc(self):
        """
        This method is called when the timeout is triggered.

        By default it calls L{loseConnection}.  Override this if you want
        something else to happen.
        """
        self.loseConnection()



class TimeoutFactory(WrappingFactory):
    """
    Factory for TimeoutWrapper.

    Stability: Unstable
    """
    protocol = TimeoutProtocol


    def __init__(self, wrappedFactory, timeoutPeriod=30*60):
        self.timeoutPeriod = timeoutPeriod
        WrappingFactory.__init__(self, wrappedFactory)


    def buildProtocol(self, addr):
        return self.protocol(self, self.wrappedFactory.buildProtocol(addr),
                             timeoutPeriod=self.timeoutPeriod)


    def callLater(self, period, func):
        """
        Wrapper around L{reactor.callLater} for test purpose.
        """
        return reactor.callLater(period, func)



class TrafficLoggingProtocol(ProtocolWrapper):

    def __init__(self, factory, wrappedProtocol, logfile, lengthLimit=None,
                 number=0):
        """
        @param factory: factory which created this protocol.
        @type factory: C{protocol.Factory}.
        @param wrappedProtocol: the underlying protocol.
        @type wrappedProtocol: C{protocol.Protocol}.
        @param logfile: file opened for writing used to write log messages.
        @type logfile: C{file}
        @param lengthLimit: maximum size of the datareceived logged.
        @type lengthLimit: C{int}
        @param number: identifier of the connection.
        @type number: C{int}.
        """
        ProtocolWrapper.__init__(self, factory, wrappedProtocol)
        self.logfile = logfile
        self.lengthLimit = lengthLimit
        self._number = number


    def _log(self, line):
        self.logfile.write(line + '\n')
        self.logfile.flush()


    def _mungeData(self, data):
        if self.lengthLimit and len(data) > self.lengthLimit:
            data = data[:self.lengthLimit - 12] + '<... elided>'
        return data


    # IProtocol
    def connectionMade(self):
        self._log('*')
        return ProtocolWrapper.connectionMade(self)


    def dataReceived(self, data):
        self._log('C %d: %r' % (self._number, self._mungeData(data)))
        return ProtocolWrapper.dataReceived(self, data)


    def connectionLost(self, reason):
        self._log('C %d: %r' % (self._number, reason))
        return ProtocolWrapper.connectionLost(self, reason)


    # ITransport
    def write(self, data):
        self._log('S %d: %r' % (self._number, self._mungeData(data)))
        return ProtocolWrapper.write(self, data)


    def writeSequence(self, iovec):
        self._log('SV %d: %r' % (self._number, [self._mungeData(d) for d in iovec]))
        return ProtocolWrapper.writeSequence(self, iovec)


    def loseConnection(self):
        self._log('S %d: *' % (self._number,))
        return ProtocolWrapper.loseConnection(self)



class TrafficLoggingFactory(WrappingFactory):
    protocol = TrafficLoggingProtocol

    _counter = 0

    def __init__(self, wrappedFactory, logfilePrefix, lengthLimit=None):
        self.logfilePrefix = logfilePrefix
        self.lengthLimit = lengthLimit
        WrappingFactory.__init__(self, wrappedFactory)


    def open(self, name):
        return file(name, 'w')


    def buildProtocol(self, addr):
        self._counter += 1
        logfile = self.open(self.logfilePrefix + '-' + str(self._counter))
        return self.protocol(self, self.wrappedFactory.buildProtocol(addr),
                             logfile, self.lengthLimit, self._counter)


    def resetCounter(self):
        """
        Reset the value of the counter used to identify connections.
        """
        self._counter = 0



class TimeoutMixin:
    """Mixin for protocols which wish to timeout connections

    @cvar timeOut: The number of seconds after which to timeout the connection.
    """
    timeOut = None

    __timeoutCall = None

    def callLater(self, period, func):
        return reactor.callLater(period, func)


    def resetTimeout(self):
        """Reset the timeout count down"""
        if self.__timeoutCall is not None and self.timeOut is not None:
            self.__timeoutCall.reset(self.timeOut)

    def setTimeout(self, period):
        """Change the timeout period

        @type period: C{int} or C{NoneType}
        @param period: The period, in seconds, to change the timeout to, or
        C{None} to disable the timeout.
        """
        prev = self.timeOut
        self.timeOut = period

        if self.__timeoutCall is not None:
            if period is None:
                self.__timeoutCall.cancel()
                self.__timeoutCall = None
            else:
                self.__timeoutCall.reset(period)
        elif period is not None:
            self.__timeoutCall = self.callLater(period, self.__timedOut)

        return prev

    def __timedOut(self):
        self.__timeoutCall = None
        self.timeoutConnection()

    def timeoutConnection(self):
        """Called when the connection times out.
        Override to define behavior other than dropping the connection.
        """
        self.transport.loseConnection()
