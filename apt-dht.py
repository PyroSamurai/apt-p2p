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

from apt_dht.apt_dht import AptDHT
from apt_dht.apt_dht_conf import config, version

config_file = []

if __name__ == '__main__':
    # Parse command line parameters when started on command line
    class AptDHTOptions(usage.Options):
        optFlags = [
            ['help', 'h'],
            ]
        optParameters = [
            ['config-file', 'c', [], "Configuration file"],
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

config.read(config_file)
if config.defaults()['username']:
    uid,gid = pwd.getpwnam(config.defaults()['username'])[2:4]
else:
    uid,gid = None,None

application = service.Application("apt-dht", uid, gid)
print service.IProcess(application).processName
service.IProcess(application).processName = 'apt-dht'

myapp = AptDHT()
site = myapp.getSite()
s = strports.service('tcp:'+config.defaults()['port'], channel.HTTPFactory(site))
s.setServiceParent(application)

if __name__ == '__main__':
    # Run on command line
    log.startLogging(sys.stdout, setStdout=0)
    service.IServiceCollection(application).privilegedStartService()
    service.IServiceCollection(application).startService()
    reactor.run()
