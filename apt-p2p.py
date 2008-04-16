#!/usr/bin/env python

# Load apt-p2p application
#
# There are two ways apt-p2p can be started:
#  1. twistd -y apt-p2p
#     - twistd will load this file and execute the app
#       in 'application' variable
#  2. from command line
#     - __name__ will be '__main__'

import pwd,sys

from twisted.application import service, internet, app, strports
from twisted.internet import reactor
from twisted.python import usage, log

from apt_p2p.apt_p2p_conf import config, version, DEFAULT_CONFIG_FILES
from apt_p2p.interfaces import IDHT, IDHTStatsFactory

config_file = ''

if __name__ == '__main__':
    # Parse command line parameters when started on command line
    class AptP2POptions(usage.Options):
        optFlags = [
            ['help', 'h', 'Print this help message'],
            ]
        optParameters = [
            ['config-file', 'c', '', "Configuration file"],
            ['log-file', 'l', '-', "File to log to, - for stdout"],
            ]
        longdesc="apt-p2p is a peer-to-peer downloader for apt users"
        def opt_version(self):
            print "apt-p2p %s" % version.short()
            sys.exit(0)

    opts = AptP2POptions()
    try:
        opts.parseOptions()
    except usage.UsageError, ue:
        print '%s: %s' % (sys.argv[0], ue)
        sys.exit(1)

    config_file = opts.opts['config-file']
    log_file = opts.opts['log-file']
    if log_file == '-':
        f = sys.stdout
    else:
        f = open(log_file, 'w')
    log.startLogging(f, setStdout=1)

log.msg("Loading config files: '%s'" % "', '".join(DEFAULT_CONFIG_FILES + [config_file]))
config_read = config.read(DEFAULT_CONFIG_FILES + [config_file])
log.msg("Successfully loaded config files: '%s'" % "', '".join(config_read))
try:
    uid,gid = pwd.getpwnam(config.get('DEFAULT', 'USERNAME'))[2:4]
except:
    uid,gid = None,None

log.msg('Starting application')
application = service.Application("apt-p2p", uid, gid)
#print service.IProcess(application).processName
#service.IProcess(application).processName = 'apt-p2p'

log.msg('Starting DHT')
DHT = __import__(config.get('DEFAULT', 'DHT')+'.DHT', globals(), locals(), ['DHT'])
assert IDHT.implementedBy(DHT.DHT), "You must provide a DHT implementation that implements the IDHT interface."

if not config.getboolean('DEFAULT', 'DHT-only'):
    log.msg('Starting main application server')
    from apt_p2p.apt_p2p import AptP2P
    myapp = AptP2P(DHT.DHT)
    factory = myapp.getHTTPFactory()
    s = strports.service('tcp:'+config.get('DEFAULT', 'port'), factory)
    s.setServiceParent(application)
else:
    myDHT = DHT.DHT()
    if IDHTStatsFactory.implementedBy(DHT.DHT):
        log.msg("Starting the DHT's HTTP stats displayer")
        factory = myDHT.getStatsFactory()
        s = strports.service('tcp:'+config.get('DEFAULT', 'port'), factory)
        s.setServiceParent(application)
        
    myDHT.loadConfig(config, config.get('DEFAULT', 'DHT'))
    myDHT.join()

if __name__ == '__main__':
    # Run on command line
    service.IServiceCollection(application).privilegedStartService()
    service.IServiceCollection(application).startService()
    reactor.run()
