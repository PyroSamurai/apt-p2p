
from twisted.internet.default import SelectReactor
reactor = SelectReactor(installSignalHandlers=0)
from twisted.internet import main
main.installReactor(reactor)