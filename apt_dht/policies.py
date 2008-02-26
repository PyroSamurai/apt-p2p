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

    def write(self, data):
        self.factory.registerWritten(len(data))
        ProtocolWrapper.write(self, data)

    def writeSequence(self, seq):
        self.factory.registerWritten(reduce(operator.add, map(len, seq)))
        ProtocolWrapper.writeSequence(self, seq)

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

    def throttleWrites(self):
        if hasattr(self, "producer"):
            self.producer.pauseProducing()

    def unthrottleWrites(self):
        if hasattr(self, "producer"):
            self.producer.resumeProducing()


class ThrottlingFactory(WrappingFactory):
    """Throttles bandwidth and number of connections.

    Write bandwidth will only be throttled if there is a producer
    registered.
    """

    protocol = ThrottlingProtocol

    def __init__(self, wrappedFactory, maxConnectionCount=sys.maxint, readLimit=None, writeLimit=None):
        WrappingFactory.__init__(self, wrappedFactory)
        self.connectionCount = 0
        self.maxConnectionCount = maxConnectionCount
        self.readLimit = readLimit # max bytes we should read per second
        self.writeLimit = writeLimit # max bytes we should write per second
        self.readThisSecond = 0
        self.writtenThisSecond = 0
        self.unthrottleReadsID = None
        self.checkReadBandwidthID = None
        self.unthrottleWritesID = None
        self.checkWriteBandwidthID = None

    def registerWritten(self, length):
        """Called by protocol to tell us more bytes were written."""
        self.writtenThisSecond += length

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
        if self.writtenThisSecond > self.writeLimit:
            self.throttleWrites()
            throttleTime = (float(self.writtenThisSecond) / self.writeLimit) - 1.0
            self.unthrottleWritesID = reactor.callLater(throttleTime,
                                                        self.unthrottleWrites)
        # reset for next round
        self.writtenThisSecond = 0
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

    def throttleWrites(self):
        """Throttle writes on all protocols."""
        log.msg("Throttling writes on %s" % self)
        for p in self.protocols.keys():
            p.throttleWrites()

    def unthrottleWrites(self):
        """Stop throttling writes on all protocols."""
        self.unthrottleWritesID = None
        log.msg("Stopped throttling writes on %s" % self)
        for p in self.protocols.keys():
            p.unthrottleWrites()

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

