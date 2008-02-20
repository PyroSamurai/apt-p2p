#!/usr/bin/env python

# Load apt-dht application
#
# There are two ways apt-dht can be started:
#  1. twistd -y apt-dht
#     - twistd will load this file and execute the app
#       in 'application' variable
#  2. from command line
#     - __name__ will be '__main__'

import pwd,sys

from twisted.application import service, internet, app, strports
from twisted.internet import reactor
from twisted.python import usage, log
from twisted.web2 import channel

from apt_dht.apt_dht_conf import config, version, DEFAULT_CONFIG_FILES
from apt_dht.interfaces import IDHT

config_file = ''

if __name__ == '__main__':
    # Parse command line parameters when started on command line
    class AptDHTOptions(usage.Options):
        optFlags = [
            ['help', 'h', 'Print this help message'],
            ]
        optParameters = [
            ['config-file', 'c', '', "Configuration file"],
            ['log-file', 'l', '-', "File to log to, - for stdout"],
            ]
        longdesc="apt-dht is a peer-to-peer downloader for apt users"
        def opt_version(self):
            print "apt-dht %s" % version.short()
            sys.exit(0)

    opts = AptDHTOptions()
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
if config.has_option('DEFAULT', 'username') and config.get('DEFAULT', 'username'):
    uid,gid = pwd.getpwnam(config.get('DEFAULT', 'username'))[2:4]
else:
    uid,gid = None,None

log.msg('Starting application')
application = service.Application("apt-dht", uid, gid)
#print service.IProcess(application).processName
#service.IProcess(application).processName = 'apt-dht'

log.msg('Starting DHT')
DHT = __import__(config.get('DEFAULT', 'DHT')+'.DHT', globals(), locals(), ['DHT'])
assert IDHT.implementedBy(DHT.DHT), "You must provide a DHT implementation that implements the IDHT interface."
myDHT = DHT.DHT()

if not config.getboolean('DEFAULT', 'DHT-only'):
    log.msg('Starting main application server')
    from apt_dht.apt_dht import AptDHT
    myapp = AptDHT(myDHT)
    factory = myapp.getHTTPFactory()
    s = strports.service('tcp:'+config.get('DEFAULT', 'port'), factory)
    s.setServiceParent(application)
else:
    myDHT.loadConfig(config, config.get('DEFAULT', 'DHT'))
    myDHT.join()

if __name__ == '__main__':
    # Run on command line
    service.IServiceCollection(application).privilegedStartService()
    service.IServiceCollection(application).startService()
    reactor.run()
