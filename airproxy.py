from airhook import listenAirhookStream, StreamConnection

from twisted.internet import protocol, reactor
import sys
from random import randrange

#### airhook -> tcp
class UDPListener(protocol.Protocol):
    started = 0
    def makeConnection(self, connection):
        self.conn = connection
    def dataReceived(self, data):
        if not self.started:
            if data == '\03BAP':
                self.started = 1
        else:
            self.out.transport.write(data)
        
class TCPReceiver(protocol.Protocol):
    def dataReceived(self, data):
        self.out.conn.write(data)

class TCPOutFactory(protocol.ClientFactory):
    protocol = TCPReceiver
    def __init__(self, out):
        self.out = out
    def buildProtocol(self, addr):
        p = protocol.ClientFactory.buildProtocol(self, addr)
        p.out = self.out
        p.out.out = p
        return p

class AirUDPProxyFactory(protocol.ServerFactory):
    oport = 0
    protocol = UDPListener
    def __init__(self, out):
        self.out = out
    def buildProtocol(self, addr):
        p = protocol.ServerFactory.buildProtocol(self, addr)
        reactor.connectTCP('localhost', self.out, TCPOutFactory(p))
        return p

def remote(udp, tcp):
    f = AirUDPProxyFactory(tcp)
    ah = listenAirhookStream(udp, f)


######  tcp -> airhook
class UDPReceiver(protocol.Protocol):
    def __init__(self, tcp):
        self.tcp = tcp
    def dataReceived(self, data):
        self.tcp.transport.write(data)
    def makeConnection(self, conn):
        self.tcp.out = conn
        conn.write("\03BAP")

class TCPListener(protocol.Protocol):
    def dataReceived(self, data):
        self.out.write(data)

class UDPOutFactory(protocol.ClientFactory):
    protocol = UDPReceiver
    def __init__(self, out):
        self.out = out
    def buildProtocol(self, addr):
        p = UDPReceiver(self.out)
        return p
        
class AirTCPProxyFactory(protocol.ServerFactory):
    oaddr = ('',0)
    protocol = TCPListener
    def __init__(self, oaddr):
        self.oaddr = oaddr
    def buildProtocol(self, addr):
        p = TCPListener()
        ah = listenAirhookStream(randrange(10000,12000), UDPOutFactory(p))
        reactor.iterate()
        c = ah.connectionForAddr(self.oaddr)
        #c.noisy= 1 
        return p
        
def local(tcp, udp):
    f = AirTCPProxyFactory(('64.81.64.214', udp))
    reactor.listenTCP(tcp, f)
    
if __name__ == '__main__':
    if sys.argv[1] == '-l':
        local(int(sys.argv[2]), int(sys.argv[3]))
    else:
        remote(int(sys.argv[2]), int(sys.argv[3]))
    reactor.run()
