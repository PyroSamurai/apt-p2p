#!/usr/bin/env python

"""Automated tests of the apt-p2p functionality.

This script runs several automatic tests of some of the functionality in
the apt-p2p program.

@type tests: C{dictionary}
@var tests: all of the tests that can be run.
    The keys are the test names (strings) which are used on the command-line
    to identify the tests (can not be 'all' or 'help'). The values are tuples
    with four elements: a description of the test (C{string}), the bootstrap
    nodes to start (C{dictionary}), the downloaders to start (C{dictionary},
    and the apt-get commands to run (C{list}).
    
    The bootstrap nodes keys are integers, which must be in the range 1-9.
    The values are the dictionary of keyword options to pass to the function
    that starts the bootstrap node (see L{start_bootstrap} below).
    
    The downloaders keys are also integers in the range 1-99. The values are
    the dictionary of keyword options to pass to the function
    that starts the downloader node (see L{start_downloader} below).
    
    The apt-get commands' list elements are tuples with 2 elements: the
    downloader to run the command on, and the list of command-line
    arguments to specify to the apt-get program.
    
@type CWD: C{string}
@var CWD: the working directory the script was run from
@type apt_conf_template: C{string}
@var apt_conf_template: the template to use for the apt.conf file

"""

from time import sleep, time
import sys, os, signal
from traceback import print_exc
from os.path import exists

tests = {'1': ('Start a single bootstrap and downloader, test updating and downloading ' +
             'using HTTP only.',
             {1: {}},
             {1: {}},
             [(1, ['update']), 
              (1, ['install', 'aboot-base']),
              (1, ['install', 'aap-doc']),
              (1, ['install', 'ada-reference-manual']),
              (1, ['install', 'aspectj-doc']),
              (1, ['install', 'fop-doc']),
              (1, ['install', 'jswat-doc']),
              (1, ['install', 'asis-doc']),
              (1, ['install', 'bison-doc']),
              (1, ['install', 'crash-whitepaper']),
              ]),

         '2': ('Start a single bootstrap and 2 downloaders to test downloading from a peer.',
               {1: {}},
               {1: {},
                2: {}},
               [(1, ['update']),
                (2, ['update']),
                (1, ['install', 'aboot-base']),
                (2, ['install', 'aboot-base']),
                (1, ['install', 'aap-doc']),
                (1, ['install', 'ada-reference-manual']),
                (1, ['install', 'fop-doc']),
                (1, ['install', 'jswat-doc']),
                (1, ['install', 'bison-doc']),
                (1, ['install', 'crash-whitepaper']),
                (2, ['install', 'aap-doc']),
                (2, ['install', 'ada-reference-manual']),
                (2, ['install', 'fop-doc']),
                (2, ['install', 'jswat-doc']),
                (2, ['install', 'bison-doc']),
                (2, ['install', 'crash-whitepaper']),
                ]),
                
         '3': ('Start a single bootstrap and 6 downloaders, to test downloading' +
               ' speeds from each other.',
               {1: {}},
               {1: {},
                2: {},
                3: {},
                4: {},
                5: {},
                6: {}},
               [(1, ['update']),
                (1, ['install', 'aboot-base']),
                (1, ['install', 'ada-reference-manual']),
                (1, ['install', 'fop-doc']),
                (1, ['install', 'crash-whitepaper']),
                (2, ['update']),
                (2, ['install', 'aboot-base']),
                (2, ['install', 'ada-reference-manual']),
                (2, ['install', 'fop-doc']),
                (2, ['install', 'crash-whitepaper']),
                (3, ['update']),
                (3, ['install', 'aboot-base']),
                (3, ['install', 'ada-reference-manual']),
                (3, ['install', 'fop-doc']),
                (3, ['install', 'crash-whitepaper']),
                (4, ['update']),
                (4, ['install', 'aboot-base']),
                (4, ['install', 'ada-reference-manual']),
                (4, ['install', 'fop-doc']),
                (4, ['install', 'crash-whitepaper']),
                (5, ['update']),
                (5, ['install', 'aboot-base']),
                (5, ['install', 'ada-reference-manual']),
                (5, ['install', 'fop-doc']),
                (5, ['install', 'crash-whitepaper']),
                (6, ['update']),
                (6, ['install', 'aboot-base']),
                (6, ['install', 'ada-reference-manual']),
                (6, ['install', 'fop-doc']),
                (6, ['install', 'crash-whitepaper']),
                ]),

         '4': ('Start a single bootstrap and 1 downloader, requesting the same' +
               ' packages multiple times to test caching.',
               {1: {}},
               {1: {}},
               [(1, ['update']),
                (1, ['install', 'aboot-base']),
                (1, ['install', 'ada-reference-manual']),
                (1, ['install', 'fop-doc']),
                (1, ['install', 'crash-whitepaper']),
                (1, ['update']),
                (1, ['install', 'aboot-base']),
                (1, ['install', 'ada-reference-manual']),
                (1, ['install', 'fop-doc']),
                (1, ['install', 'crash-whitepaper']),
                (1, ['update']),
                (1, ['install', 'aboot-base']),
                (1, ['install', 'ada-reference-manual']),
                (1, ['install', 'fop-doc']),
                (1, ['install', 'crash-whitepaper']),
                ]),
                
         '5': ('Start a single bootstrap and 6 downloaders, update all to test' +
               ' that they can all see each other.',
               {1: {}},
               {1: ([], {'suites': 'contrib non-free'}),
                2: ([], {'suites': 'contrib non-free'}),
                3: ([], {'suites': 'contrib non-free'}),
                4: ([], {'suites': 'contrib non-free'}),
                5: ([], {'suites': 'contrib non-free'}),
                6: ([], {'suites': 'contrib non-free'})},
               [(1, ['update']),
                (2, ['update']),
                (3, ['update']),
                (4, ['update']),
                (5, ['update']),
                (6, ['update']),
                ]),

        '6': ('Test caching with multiple apt-get updates.',
             {1: {}},
             {1: {}},
             [(1, ['update']), 
              (1, ['update']),
              (1, ['update']),
              (1, ['update']),
              ]),

        '7': ('Test pipelining of multiple simultaneous downloads.',
             {1: {}},
             {1: {}},
             [(1, ['update']), 
              (1, ['install', 'aboot-base', 'aap-doc', 'ada-reference-manual',
                   'aspectj-doc', 'fop-doc', 'asis-doc',
                   'bison-doc', 'crash-whitepaper',
                   'bash-doc', 'apt-howto-common', 'autotools-dev',
                   'aptitude-doc-en', 'asr-manpages',
                   'atomix-data', 'alcovebook-sgml-doc',
                   'afbackup-common', 'airstrike-common',
                   ]),
              ]),

        '8': ('Test pipelining of multiple simultaneous downloads with many peers.',
             {1: {}},
             {1: {},
              2: {},
              3: {},
              4: {},
              5: {},
              6: {}},
             [(1, ['update']), 
              (1, ['install', 'aboot-base', 'aap-doc', 'ada-reference-manual',
                   'aspectj-doc', 'fop-doc', 'asis-doc',
                   'bison-doc', 'crash-whitepaper',
                   'bash-doc', 'apt-howto-common', 'autotools-dev',
                   'aptitude-doc-en', 'asr-manpages',
                   'atomix-data', 'alcovebook-sgml-doc',
                   'afbackup-common', 'airstrike-common',
                   ]),
              (2, ['update']), 
              (2, ['install', 'aboot-base', 'aap-doc', 'ada-reference-manual',
                   'aspectj-doc', 'fop-doc', 'asis-doc',
                   'bison-doc', 'crash-whitepaper',
                   'bash-doc', 'apt-howto-common', 'autotools-dev',
                   'aptitude-doc-en', 'asr-manpages',
                   'atomix-data', 'alcovebook-sgml-doc',
                   'afbackup-common', 'airstrike-common',
                   ]),
              (3, ['update']), 
              (3, ['install', 'aboot-base', 'aap-doc', 'ada-reference-manual',
                   'aspectj-doc', 'fop-doc', 'asis-doc',
                   'bison-doc', 'crash-whitepaper',
                   'bash-doc', 'apt-howto-common', 'autotools-dev',
                   'aptitude-doc-en', 'asr-manpages',
                   'atomix-data', 'alcovebook-sgml-doc',
                   'afbackup-common', 'airstrike-common',
                   ]),
              (4, ['update']), 
              (4, ['install', 'aboot-base', 'aap-doc', 'ada-reference-manual',
                   'aspectj-doc', 'fop-doc', 'asis-doc',
                   'bison-doc', 'crash-whitepaper',
                   'bash-doc', 'apt-howto-common', 'autotools-dev',
                   'aptitude-doc-en', 'asr-manpages',
                   'atomix-data', 'alcovebook-sgml-doc',
                   'afbackup-common', 'airstrike-common',
                   ]),
              (5, ['update']), 
              (5, ['install', 'aboot-base', 'aap-doc', 'ada-reference-manual',
                   'aspectj-doc', 'fop-doc', 'asis-doc',
                   'bison-doc', 'crash-whitepaper',
                   'bash-doc', 'apt-howto-common', 'autotools-dev',
                   'aptitude-doc-en', 'asr-manpages',
                   'atomix-data', 'alcovebook-sgml-doc',
                   'afbackup-common', 'airstrike-common',
                   ]),
              (6, ['update']), 
              (6, ['install', 'aboot-base', 'aap-doc', 'ada-reference-manual',
                   'aspectj-doc', 'fop-doc', 'asis-doc',
                   'bison-doc', 'crash-whitepaper',
                   'bash-doc', 'apt-howto-common', 'autotools-dev',
                   'aptitude-doc-en', 'asr-manpages',
                   'atomix-data', 'alcovebook-sgml-doc',
                   'afbackup-common', 'airstrike-common',
                   ]),
              ]),

         '9': ('Start a single bootstrap and 6 downloaders and test downloading' +
               ' a very large file.',
               {1: {}},
               {1: {},
                2: {},
                3: {},
                4: {},
                5: {},
                6: {}},
               [(1, ['update']),
                (1, ['install', 'kde-icons-oxygen']),
                (2, ['update']),
                (2, ['install', 'kde-icons-oxygen']),
                (3, ['update']),
                (3, ['install', 'kde-icons-oxygen']),
                (4, ['update']),
                (4, ['install', 'kde-icons-oxygen']),
                (5, ['update']),
                (5, ['install', 'kde-icons-oxygen']),
                (6, ['update']),
                (6, ['install', 'kde-icons-oxygen']),
                ]),

         }

assert 'all' not in tests
assert 'help' not in tests

CWD = os.getcwd()
apt_conf_template = """
{
  // Location of the state dir
  State "var/lib/apt/"
  {
     Lists "lists/";
     xstatus "xstatus";
     userstatus "status.user";
     cdroms "cdroms.list";
  };

  // Location of the cache dir
  Cache "var/cache/apt/" {
     Archives "archives/";
     srcpkgcache "srcpkgcache.bin";
     pkgcache "pkgcache.bin";
  };

  // Config files
  Etc "etc/apt/" {
     SourceList "sources.list";
     Main "apt.conf";
     Preferences "preferences";
     Parts "apt.conf.d/";
  };

  // Locations of binaries
  Bin {
     methods "/usr/lib/apt/methods/";
     gzip "/bin/gzip";
     gpg  "/usr/bin/gpgv";
     dpkg "/usr/bin/dpkg --simulate";
     dpkg-source "/usr/bin/dpkg-source";
     dpkg-buildpackage "/usr/bin/dpkg-buildpackage";
     apt-get "/usr/bin/apt-get";
     apt-cache "/usr/bin/apt-cache";
  };
};

/* Options you can set to see some debugging text They correspond to names
   of classes in the source code */
Debug
{
  pkgProblemResolver "false";
  pkgDepCache::AutoInstall "false"; // what packages apt install to satify dependencies
  pkgAcquire "false";
  pkgAcquire::Worker "false";
  pkgAcquire::Auth "false";
  pkgDPkgPM "false";
  pkgDPkgProgressReporting "false";
  pkgOrderList "false";
  BuildDeps "false";

  pkgInitialize "false";   // This one will dump the configuration space
  NoLocking "false";
  Acquire::Ftp "false";    // Show ftp command traffic
  Acquire::Http "false";   // Show http command traffic
  Acquire::gpgv "false";   // Show the gpgv traffic
  aptcdrom "false";        // Show found package files
  IdentCdrom "false";

}
"""
apt_p2p_conf_template = """
[DEFAULT]

# Port to listen on for all requests (TCP and UDP)
PORT = %(PORT)s
    
# The rate to limit sending data to peers to, in KBytes/sec.
# Set this to 0 to not limit the upload bandwidth.
UPLOAD_LIMIT = 100

# The minimum number of peers before the mirror is not used.
# If there are fewer peers than this for a file, the mirror will also be
# used to speed up the download. Set to 0 to never use the mirror if
# there are peers.
MIN_DOWNLOAD_PEERS = 3

# Directory to store the downloaded files in
CACHE_DIR = %(CACHE_DIR)s
    
# Other directories containing packages to share with others
# WARNING: all files in these directories will be hashed and available
#          for everybody to download
# OTHER_DIRS = 
    
# Whether it's OK to use an IP addres from a known local/private range
LOCAL_OK = yes

# Whether a remote peer can access the statistics page
REMOTE_STATS = yes

# Unload the packages cache after an interval of inactivity this long.
# The packages cache uses a lot of memory, and only takes a few seconds
# to reload when a new request arrives.
UNLOAD_PACKAGES_CACHE = 5m

# Refresh the DHT keys after this much time has passed.
# This should be a time slightly less than the DHT's KEY_EXPIRE value.
KEY_REFRESH = 57m

# Which DHT implementation to use.
# It must be possile to do "from <DHT>.DHT import DHT" to get a class that
# implements the IDHT interface.
DHT = apt_p2p_Khashmir

# Whether to only run the DHT (for providing only a bootstrap node)
DHT-ONLY = %(DHT-ONLY)s

[apt_p2p_Khashmir]
# bootstrap nodes to contact to join the DHT
BOOTSTRAP = %(BOOTSTRAP)s

# whether this node is a bootstrap node
BOOTSTRAP_NODE = %(BOOTSTRAP_NODE)s

# checkpoint every this many seconds
CHECKPOINT_INTERVAL = 5m

# concurrent xmlrpc calls per find node/value request!
CONCURRENT_REQS = 4

# how many hosts to post to
STORE_REDUNDANCY = 3

# How many values to attempt to retrieve from the DHT.
# Setting this to 0 will try and get all values (which could take a while if
# a lot of nodes have values). Setting it negative will try to get that
# number of results from only the closest STORE_REDUNDANCY nodes to the hash.
# The default is a large negative number so all values from the closest
# STORE_REDUNDANCY nodes will be retrieved.
RETRIEVE_VALUES = -10000

# how many times in a row a node can fail to respond before it's booted from the routing table
MAX_FAILURES = 3

# never ping a node more often than this
MIN_PING_INTERVAL = 15m

# refresh buckets that haven't been touched in this long
BUCKET_STALENESS = 1h

# expire entries older than this
KEY_EXPIRE = 1h

# whether to spew info about the requests/responses in the protocol
SPEW = yes
"""

def rmrf(top):
    """Remove all the files and directories below a top-level one.
    
    @type top: C{string}
    @param top: the top-level directory to start at
    
    """
    
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))

def join(dir):
    """Join together a list of directories into a path string.
    
    @type dir: C{list} of C{string}
    @param dir: the path to join together
    @rtype: C{string}
    @return: the joined together path
    
    """
    
    joined = ''
    for i in dir:
        joined = os.path.join(joined, i)
    return joined

def makedirs(dir):
    """Create all the directories to make a path.
    
    @type dir: C{list} of C{string}
    @param dir: the path to create
    
    """
    if not os.path.exists(join(dir)):
        os.makedirs(join(dir))

def touch(path):
    """Create an empty file.
    
    @type path: C{list} of C{string}
    @param path: the path to create
    
    """
    
    f = open(join(path), 'w')
    f.close()

def start(cmd, args, work_dir = None):
    """Fork and start a background process running.
    
    @type cmd: C{string}
    @param cmd: the name of the command to run
    @type args: C{list} of C{string}
    @param args: the argument to pass to the command
    @type work_dir: C{string}
    @param work_dir: the directory to change to to execute the child process in
        (optional, defaults to the current directory)
    @rtype: C{int}
    @return: the PID of the forked process
    
    """
    
    new_cmd = [cmd] + args
    pid = os.spawnvp(os.P_NOWAIT, new_cmd[0], new_cmd)
    return pid

def stop(pid):
    """Stop a forked background process that is running.
    
    @type pid: C{int}
    @param pid: the PID of the process to stop
    @rtype: C{int}
    @return: the return status code from the child
    
    """

    # First try a keyboard interrupt
    os.kill(pid, signal.SIGINT)
    for i in xrange(5):
        sleep(1)
        (r_pid, r_value) = os.waitpid(pid, os.WNOHANG)
        if r_pid:
            return r_value
    
    # Try a keyboard interrupt again, just in case
    os.kill(pid, signal.SIGINT)
    for i in xrange(5):
        sleep(1)
        (r_pid, r_value) = os.waitpid(pid, os.WNOHANG)
        if r_pid:
            return r_value

    # Try a terminate
    os.kill(pid, signal.SIGTERM)
    for i in xrange(5):
        sleep(1)
        (r_pid, r_value) = os.waitpid(pid, os.WNOHANG)
        if r_pid:
            return r_value

    # Finally a kill, don't return until killed
    os.kill(pid, signal.SIGKILL)
    while not r_pid:
        sleep(1)
        (r_pid, r_value) = os.waitpid(pid, os.WNOHANG)

    return r_value

def apt_get(num_down, cmd):
    """Start an apt-get process in the background.

    The default argument specified to the apt-get invocation are
    'apt-get -d -q -c <conf_file>'. Any additional arguments (including
    the apt-get action to use) should be specified.
    
    @type num_down: C{int}
    @param num_down: the number of the downloader to use
    @type cmd: C{list} of C{string}
    @param cmd: the arguments to pass to the apt-get process
    @rtype: C{int}
    @return: the PID of the background process
    
    """
    
    print '*************** apt-get (' + str(num_down) + ') ' + ' '.join(cmd) + ' ****************'
    apt_conf = join([down_dir(num_down), 'etc', 'apt', 'apt.conf'])
    dpkg_status = join([down_dir(num_down), 'var', 'lib', 'dpkg', 'status'])
    args = ['-d', '-c', apt_conf, '-o', 'Dir::state::status='+dpkg_status] + cmd
    pid = start('apt-get', args)
    return pid

def bootstrap_address(num_boot):
    """Determine the bootstrap address to use for a node.
    
    @type num_boot: C{int}
    @param num_boot: the number of the bootstrap node
    @rtype: C{string}
    @return: the bootstrap address to use
    
    """
    
    return 'localhost:1' + str(num_boot) + '969'

def down_dir(num_down):
    """Determine the working directory to use for a downloader.
    
    @type num_down: C{int}
    @param num_down: the number of the downloader
    @rtype: C{string}
    @return: the downloader's directory
    
    """
    
    return os.path.join(CWD,'downloader' + str(num_down))

def boot_dir(num_boot):
    """Determine the working directory to use for a bootstrap node.
    
    @type num_boot: C{int}
    @param num_boot: the number of the bootstrap node
    @rtype: C{string}
    @return: the bootstrap node's directory
    
    """
    
    return os.path.join(CWD,'bootstrap' + str(num_boot))

def start_downloader(bootstrap_addresses, num_down, options = {},
                     mirror = 'ftp.us.debian.org/debian', 
                     suites = 'main contrib non-free', clean = True):
    """Initialize a new downloader process.

    The default arguments specified to the downloader invocation are
    the configuration directory, apt port, minport, maxport and the
    maximum upload rate. 
    Any additional arguments needed should be specified by L{options}.
    
    @type num_down: C{int}
    @param num_down: the number of the downloader to use
    @type options: C{dictionary}
    @param options: the dictionary of string formatting values for creating
        the apt-p2p configuration file (see L{apt_p2p_conf_template} above).
        (optional, defaults to only using the default arguments)
    @type mirror: C{string}
    @param mirror: the Debian mirror to use
        (optional, defaults to 'ftp.us.debian.org/debian')
    @type suites: C{string}
    @param suites: space separated list of suites to download
        (optional, defaults to 'main contrib non-free')
    @type clean: C{boolean}
    @param clean: whether to remove any previous downloader files
        (optional, defaults to removing them)
    @rtype: C{int}
    @return: the PID of the downloader process
    
    """
    
    assert num_down < 100
    
    print '************************** Starting Downloader ' + str(num_down) + ' **************************'

    downloader_dir = down_dir(num_down)
    
    if clean:
        try:
            rmrf(downloader_dir)
        except:
            pass
    
        # Create the directory structure needed by apt
        makedirs([downloader_dir, 'etc', 'apt', 'apt.conf.d'])
        makedirs([downloader_dir, 'var', 'lib', 'apt', 'lists', 'partial'])
        makedirs([downloader_dir, 'var', 'lib', 'dpkg'])
        makedirs([downloader_dir, 'var', 'cache', 'apt', 'archives', 'partial'])
        touch([downloader_dir, 'var', 'lib', 'apt', 'lists', 'lock'])
        touch([downloader_dir, 'var', 'lib', 'dpkg', 'lock'])
        touch([downloader_dir, 'var', 'lib', 'dpkg', 'status'])
        touch([downloader_dir, 'var', 'cache', 'apt', 'archives', 'lock'])

        # Create apt's config files
        f = open(join([downloader_dir, 'etc', 'apt', 'sources.list']), 'w')
        f.write('deb http://localhost:1%02d77/%s/ unstable %s\n' % (num_down, mirror, suites))
        f.close()

        f = open(join([downloader_dir, 'etc', 'apt', 'apt.conf']), 'w')
        f.write('Dir "' + downloader_dir + '"')
        f.write(apt_conf_template)
        f.close()

    defaults = {'PORT': '1%02d77' % num_down,
                'CACHE_DIR': downloader_dir,
                'DHT-ONLY': 'no',
                'BOOTSTRAP': bootstrap_addresses,
                'BOOTSTRAP_NODE': 'no'}

    for k in options:
        defaults[k] = options[k]
    f = open(join([downloader_dir, 'apt-p2p.conf']), 'w')
    f.write(apt_p2p_conf_template % defaults)
    f.close()
    
    pid = start('python', [join([sys.path[0], 'apt-p2p.py']),
                           '--config-file=' + join([downloader_dir, 'apt-p2p.conf']),
                           '--log-file=' + join([downloader_dir, 'apt-p2p.log']),],
                downloader_dir)
    return pid

def start_bootstrap(bootstrap_addresses, num_boot, options = [], clean = True):
    """Initialize a new bootstrap node process.

    The default arguments specified to the apt-p2p invocation are
    the state file and port to use. Any additional arguments needed 
    should be specified by L{options}.
    
    @type num_boot: C{int}
    @param num_boot: the number of the bootstrap node to use
    @type options: C{list} of C{string}
    @param options: the arguments to pass to the bootstrap node
        (optional, defaults to only using the default arguments)
    @type clean: C{boolean}
    @param clean: whether to remove any previous bootstrap node files
        (optional, defaults to removing them)
    @rtype: C{int}
    @return: the PID of the downloader process
    
    """
    
    assert num_boot < 10

    print '************************** Starting Bootstrap ' + str(num_boot) + ' **************************'

    bootstrap_dir = boot_dir(num_boot)
    
    if clean:
        try:
            rmrf(bootstrap_dir)
        except:
            pass

    makedirs([bootstrap_dir])

    defaults = {'PORT': '1%d969' % num_boot,
                'CACHE_DIR': bootstrap_dir,
                'DHT-ONLY': 'yes',
                'BOOTSTRAP': bootstrap_addresses,
                'BOOTSTRAP_NODE': 'yes'}

    for k in options:
        defaults[k] = options[k]
    f = open(join([bootstrap_dir, 'apt-p2p.conf']), 'w')
    f.write(apt_p2p_conf_template % defaults)
    f.close()
    
    pid = start('python', [join([sys.path[0], 'apt-p2p.py']),
                           '--config-file=' + join([bootstrap_dir, 'apt-p2p.conf']),
                           '--log-file=' + join([bootstrap_dir, 'apt-p2p.log']),],
                bootstrap_dir)

    return pid

def run_test(bootstraps, downloaders, apt_get_queue):
    """Run a single test.
    
    @type bootstraps: C{dictionary} of {C{int}: C{list} of C{string}}
    @param bootstraps: the bootstrap nodes to start, keys are the bootstrap numbers and
        values are the list of options to invoke the bootstrap node with
    @type downloaders: C{dictionary} of {C{int}: (C{int}, C{list} of C{string})}
    @param downloaders: the downloaders to start, keys are the downloader numbers and
        values are the list of options to invoke the downloader with
    @type apt_get_queue: C{list} of (C{int}, C{list} of C{string})
    @param apt_get_queue: the apt-get downloader to use and commands to execute
    @rtype: C{list} of (C{float}, C{int})
    @return: the execution time and returned status code for each element of apt_get_queue
    
    """
    
    running_bootstraps = {}
    running_downloaders = {}
    running_apt_get = {}
    apt_get_results = []

    try:
        boot_keys = bootstraps.keys()
        boot_keys.sort()
        bootstrap_addresses = bootstrap_address(boot_keys[0])
        for i in xrange(1, len(boot_keys)):
            bootstrap_addresses += '\n      ' + bootstrap_address(boot_keys[i])
            
        for k, v in bootstraps.items():
            running_bootstraps[k] = start_bootstrap(bootstrap_addresses, k, **v)
        
        sleep(5)
        
        for k, v in downloaders.items():
            running_downloaders[k] = start_downloader(bootstrap_addresses, k, **v)
    
        sleep(5)
        
        for (num_down, cmd) in apt_get_queue:
            running_apt_get[num_down] = apt_get(num_down, cmd)
            start_time = time()
            (pid, r_value) = os.waitpid(running_apt_get[num_down], 0)
            elapsed = time() - start_time
            del running_apt_get[num_down]
            r_value = r_value / 256
            apt_get_results.append((elapsed, r_value))

            if r_value == 0:
                print '********** apt-get completed successfully in ' +  str(elapsed) + ' sec. *****************'
            else:
                print '********** apt-get finished with status ' + str(r_value) + ' in ' +  str(elapsed) + ' sec. ************'
        
            sleep(5)
            
    except:
        print '************************** Exception occurred **************************'
        print_exc()
        print '************************** will attempt to shut down *******************'
        
    print '*********************** shutting down the apt-gets *******************'
    for k, v in running_apt_get.items():
        try:
            print 'apt-get', k, stop(v)
        except:
            print '************************** Exception occurred **************************'
            print_exc()

    sleep(5)

    print '*********************** shutting down the downloaders *******************'
    for k, v in running_downloaders.items():
        try:
            print 'downloader', k, stop(v)
        except:
            print '************************** Exception occurred **************************'
            print_exc()

    sleep(5)

    print '************************** shutting down the bootstraps *******************'
    for k, v in running_bootstraps.items():
        try:
            print 'bootstrap', k, stop(v)
        except:
            print '************************** Exception occurred **************************'
            print_exc()

    print '************************** Test Results *******************'
    i = -1
    for (num_down, cmd) in apt_get_queue:
        i += 1
        s = str(num_down) + ': "apt-get ' + ' '.join(cmd) + '" '
        if len(apt_get_results) > i:
            (elapsed, r_value) = apt_get_results[i]
            s += 'took ' + str(elapsed) + ' secs (' + str(r_value) + ')'
        else:
            s += 'did not complete'
        print s
    
    return apt_get_results

def get_usage():
    """Get the usage information to display to the user.
    
    @rtype: C{string}
    @return: the usage information to display
    
    """
    
    s = 'Usage: ' + sys.argv[0] + ' (all|<test>|help)\n\n'
    s += '  all    - run all the tests\n'
    s += '  help   - display this usage information\n'
    s += '  <test> - run the <test> test (see list below for valid tests)\n\n'
    
    t = tests.items()
    t.sort()
    for k, v in t:
        s += 'test "' + str(k) + '" - ' + v[0] + '\n'
    
    return s

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print get_usage()
    elif sys.argv[1] == 'all':
        for k, v in tests.items():
            run_test(v[1], v[2], v[3])
    elif sys.argv[1] in tests:
        v = tests[sys.argv[1]]
        run_test(v[1], v[2], v[3])
    elif sys.argv[1] == 'help':
        print get_usage()
    else:
        print 'Unknown test to run:', sys.argv[1], '\n'
        print get_usage()
        