from twisted.internet.default import SelectReactor ## twistedmatrix.com

reactor = SelectReactor(installSignalHandlers=0)
from twisted.internet import main
main.installReactor(reactor)