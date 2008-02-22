#
# Copyright (C) 2002 Manuel Estrada Sainz <ranty@debian.org>
# Copyright (C) 2008 Cameron Dale <camrdale@gmail.com>
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of version 2.1 of the GNU General Public
# License as published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

# Disable the FutureWarning from the apt module
import warnings
warnings.simplefilter("ignore", FutureWarning)

import os, shelve
from random import choice
from shutil import rmtree
from copy import deepcopy
from UserDict import DictMixin

from twisted.internet import threads, defer, reactor
from twisted.python import log
from twisted.python.filepath import FilePath
from twisted.trial import unittest

import apt_pkg, apt_inst
from apt import OpProgress
from debian_bundle import deb822

from Hash import HashObject

apt_pkg.init()

TRACKED_FILES = ['release', 'sources', 'packages']

class PackageFileList(DictMixin):
    """Manages a list of package files belonging to a backend.
    
    @type packages: C{shelve dictionary}
    @ivar packages: the files stored for this backend
    """
    
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self.cache_dir.restat(False)
        if not self.cache_dir.exists():
            self.cache_dir.makedirs()
        self.packages = None
        self.open()

    def open(self):
        """Open the persistent dictionary of files in this backend."""
        if self.packages is None:
            self.packages = shelve.open(self.cache_dir.child('packages.db').path)

    def close(self):
        """Close the persistent dictionary."""
        if self.packages is not None:
            self.packages.close()

    def update_file(self, cache_path, file_path):
        """Check if an updated file needs to be tracked.

        Called from the mirror manager when files get updated so we can update our
        fake lists and sources.list.
        """
        filename = cache_path.split('/')[-1]
        if filename.lower() in TRACKED_FILES:
            log.msg("Registering package file: "+cache_path)
            self.packages[cache_path] = file_path
            return True
        return False

    def check_files(self):
        """Check all files in the database to make sure they exist."""
        files = self.packages.keys()
        for f in files:
            self.packages[f].restat(False)
            if not self.packages[f].exists():
                log.msg("File in packages database has been deleted: "+f)
                del self.packages[f]

    # Standard dictionary implementation so this class can be used like a dictionary.
    def __getitem__(self, key): return self.packages[key]
    def __setitem__(self, key, item): self.packages[key] = item
    def __delitem__(self, key): del self.packages[key]
    def keys(self): return self.packages.keys()

class AptPackages:
    """Uses python-apt to answer queries about packages.

    Makes a fake configuration for python-apt for each backend.
    """

    DEFAULT_APT_CONFIG = {
        #'APT' : '',
        #'APT::Architecture' : 'i386',  # Commented so the machine's config will set this
        #'APT::Default-Release' : 'unstable',
        'Dir':'.', # /
        'Dir::State' : 'apt/', # var/lib/apt/
        'Dir::State::Lists': 'lists/', # lists/
        #'Dir::State::cdroms' : 'cdroms.list',
        'Dir::State::userstatus' : 'status.user',
        'Dir::State::status': 'dpkg/status', # '/var/lib/dpkg/status'
        'Dir::Cache' : '.apt/cache/', # var/cache/apt/
        #'Dir::Cache::archives' : 'archives/',
        'Dir::Cache::srcpkgcache' : 'srcpkgcache.bin',
        'Dir::Cache::pkgcache' : 'pkgcache.bin',
        'Dir::Etc' : 'apt/etc/', # etc/apt/
        'Dir::Etc::sourcelist' : 'sources.list',
        'Dir::Etc::vendorlist' : 'vendors.list',
        'Dir::Etc::vendorparts' : 'vendors.list.d',
        #'Dir::Etc::main' : 'apt.conf',
        #'Dir::Etc::parts' : 'apt.conf.d',
        #'Dir::Etc::preferences' : 'preferences',
        'Dir::Bin' : '',
        #'Dir::Bin::methods' : '', #'/usr/lib/apt/methods'
        'Dir::Bin::dpkg' : '/usr/bin/dpkg',
        #'DPkg' : '',
        #'DPkg::Pre-Install-Pkgs' : '',
        #'DPkg::Tools' : '',
        #'DPkg::Tools::Options' : '',
        #'DPkg::Tools::Options::/usr/bin/apt-listchanges' : '',
        #'DPkg::Tools::Options::/usr/bin/apt-listchanges::Version' : '2',
        #'DPkg::Post-Invoke' : '',
        }
    essential_dirs = ('apt', 'apt/cache', 'apt/dpkg', 'apt/etc', 'apt/lists',
                      'apt/lists/partial')
    essential_files = ('apt/dpkg/status', 'apt/etc/sources.list',)
        
    def __init__(self, cache_dir, unload_delay):
        """Construct a new packages manager.

        @param cache_dir: cache directory from config file
        """
        self.cache_dir = cache_dir
        self.unload_delay = unload_delay
        self.apt_config = deepcopy(self.DEFAULT_APT_CONFIG)

        for dir in self.essential_dirs:
            path = self.cache_dir.preauthChild(dir)
            if not path.exists():
                path.makedirs()
        for file in self.essential_files:
            path = self.cache_dir.preauthChild(file)
            if not path.exists():
                path.touch()
                
        self.apt_config['Dir'] = self.cache_dir.path
        self.apt_config['Dir::State::status'] = self.cache_dir.preauthChild(self.apt_config['Dir::State']).preauthChild(self.apt_config['Dir::State::status']).path
        self.packages = PackageFileList(cache_dir)
        self.loaded = 0
        self.loading = None
        self.unload_later = None
        
    def __del__(self):
        self.cleanup()
        self.packages.close()
        
    def addRelease(self, cache_path, file_path):
        """Dirty hack until python-apt supports apt-pkg/indexrecords.h
        (see Bug #456141)
        """
        self.indexrecords[cache_path] = {}

        read_packages = False
        f = file_path.open('r')
        
        rel = deb822.Release(f, fields = ['MD5Sum', 'SHA1', 'SHA256'])
        for hash_type in rel:
            for file in rel[hash_type]:
                self.indexrecords[cache_path].setdefault(file['name'], {})[hash_type.upper()] = (file[hash_type], file['size'])
            
        f.close()

    def file_updated(self, cache_path, file_path):
        """A file in the backend has changed, manage it.
        
        If this affects us, unload our apt database
        """
        if self.packages.update_file(cache_path, file_path):
            self.unload()

    def load(self):
        """Make sure the package is initialized and loaded."""
        if self.unload_later and self.unload_later.active():
            self.unload_later.reset(self.unload_delay)
        else:
            self.unload_later = reactor.callLater(self.unload_delay, self.unload)
        if self.loading is None:
            log.msg('Loading the packages cache')
            self.loading = threads.deferToThread(self._load)
            self.loading.addCallback(self.doneLoading)
        return self.loading
        
    def doneLoading(self, loadResult):
        """Cache is loaded."""
        self.loading = None
        # Must pass on the result for the next callback
        return loadResult
        
    def _load(self):
        """Regenerates the fake configuration and load the packages cache."""
        if self.loaded: return True
        apt_pkg.InitSystem()
        self.cache_dir.preauthChild(self.apt_config['Dir::State']
                     ).preauthChild(self.apt_config['Dir::State::Lists']).remove()
        self.cache_dir.preauthChild(self.apt_config['Dir::State']
                     ).preauthChild(self.apt_config['Dir::State::Lists']
                     ).child('partial').makedirs()
        sources_file = self.cache_dir.preauthChild(self.apt_config['Dir::Etc']
                               ).preauthChild(self.apt_config['Dir::Etc::sourcelist'])
        sources = sources_file.open('w')
        sources_count = 0
        deb_src_added = False
        self.packages.check_files()
        self.indexrecords = {}
        for f in self.packages:
            # we should probably clear old entries from self.packages and
            # take into account the recorded mtime as optimization
            file = self.packages[f]
            if f.split('/')[-1] == "Release":
                self.addRelease(f, file)
            fake_uri='http://apt-dht'+f
            fake_dirname = '/'.join(fake_uri.split('/')[:-1])
            if f.endswith('Sources'):
                deb_src_added = True
                source_line='deb-src '+fake_dirname+'/ /'
            else:
                source_line='deb '+fake_dirname+'/ /'
            listpath = self.cache_dir.preauthChild(self.apt_config['Dir::State']
                                    ).preauthChild(self.apt_config['Dir::State::Lists']
                                    ).child(apt_pkg.URItoFileName(fake_uri))
            sources.write(source_line+'\n')
            log.msg("Sources line: " + source_line)
            sources_count = sources_count + 1

            if listpath.exists():
                #we should empty the directory instead
                listpath.remove()
            os.symlink(file.path, listpath.path)
        sources.close()

        if sources_count == 0:
            log.msg("No Packages files available for %s backend"%(self.cache_dir.path))
            return False

        log.msg("Loading Packages database for "+self.cache_dir.path)
        for key, value in self.apt_config.items():
            apt_pkg.Config[key] = value

        self.cache = apt_pkg.GetCache(OpProgress())
        self.records = apt_pkg.GetPkgRecords(self.cache)
        if deb_src_added:
            self.srcrecords = apt_pkg.GetPkgSrcRecords()
        else:
            self.srcrecords = None

        self.loaded = 1
        return True

    def unload(self):
        """Tries to make the packages server quit."""
        if self.unload_later and self.unload_later.active():
            self.unload_later.cancel()
        self.unload_later = None
        if self.loaded:
            log.msg('Unloading the packages cache')
            del self.cache
            del self.records
            del self.srcrecords
            del self.indexrecords
            self.loaded = 0

    def cleanup(self):
        """Cleanup and close any loaded caches."""
        self.unload()
        self.packages.close()
        
    def findHash(self, path):
        """Find the hash for a given path in this mirror.
        
        Returns a deferred so it can make sure the cache is loaded first.
        """
        d = defer.Deferred()

        deferLoad = self.load()
        deferLoad.addCallback(self._findHash, path, d)
        deferLoad.addErrback(self._findHash_error, path, d)
        
        return d

    def _findHash_error(self, failure, path, d):
        """An error occurred while trying to find a hash."""
        log.msg('An error occurred while looking up a hash for: %s' % path)
        log.err(failure)
        d.callback(HashObject())

    def _findHash(self, loadResult, path, d):
        """Really find the hash for a path.
        
        Have to pass the returned loadResult on in case other calls to this
        function are pending.
        """
        if not loadResult:
            d.callback(HashObject())
            return loadResult
        
        # First look for the path in the cache of index files
        for release in self.indexrecords:
            if path.startswith(release[:-7]):
                for indexFile in self.indexrecords[release]:
                    if release[:-7] + indexFile == path:
                        h = HashObject()
                        h.setFromIndexRecord(self.indexrecords[release][indexFile])
                        d.callback(h)
                        return loadResult
        
        package = path.split('/')[-1].split('_')[0]

        # Check the binary packages
        try:
            for version in self.cache[package].VersionList:
                size = version.Size
                for verFile in version.FileList:
                    if self.records.Lookup(verFile):
                        if '/' + self.records.FileName == path:
                            h = HashObject()
                            h.setFromPkgRecord(self.records, size)
                            d.callback(h)
                            return loadResult
        except KeyError:
            pass

        # Check the source packages' files
        if self.srcrecords:
            self.srcrecords.Restart()
            if self.srcrecords.Lookup(package):
                for f in self.srcrecords.Files:
                    if path == '/' + f[2]:
                        h = HashObject()
                        h.setFromSrcRecord(f)
                        d.callback(h)
                        return loadResult
        
        d.callback(HashObject())
        return loadResult

class TestAptPackages(unittest.TestCase):
    """Unit tests for the AptPackages cache."""
    
    pending_calls = []
    client = None
    timeout = 10
    packagesFile = ''
    sourcesFile = ''
    releaseFile = ''
    
    def setUp(self):
        self.client = AptPackages(FilePath('/tmp/.apt-dht'), 300)
    
        self.packagesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "_main_.*Packages$" | tail -n 1').read().rstrip('\n')
        self.sourcesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "_main_.*Sources$" | tail -n 1').read().rstrip('\n')
        for f in os.walk('/var/lib/apt/lists').next()[2]:
            if f[-7:] == "Release" and self.packagesFile.startswith(f[:-7]):
                self.releaseFile = f
                break
        
        self.client.file_updated(self.releaseFile[self.releaseFile.find('_dists_'):].replace('_','/'), 
                                 FilePath('/var/lib/apt/lists/' + self.releaseFile))
        self.client.file_updated(self.packagesFile[self.packagesFile.find('_dists_'):].replace('_','/'), 
                                 FilePath('/var/lib/apt/lists/' + self.packagesFile))
        self.client.file_updated(self.sourcesFile[self.sourcesFile.find('_dists_'):].replace('_','/'), 
                                 FilePath('/var/lib/apt/lists/' + self.sourcesFile))
    
    def test_pkg_hash(self):
        self.client._load()

        self.client.records.Lookup(self.client.cache['dpkg'].VersionList[0].FileList[0])
        
        pkg_hash = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^SHA1:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')

        self.failUnless(self.client.records.SHA1Hash == pkg_hash, 
                        "Hashes don't match: %s != %s" % (self.client.records.SHA1Hash, pkg_hash))

    def test_src_hash(self):
        self.client._load()

        self.client.srcrecords.Lookup('dpkg')

        src_hashes = os.popen('grep -A 20 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -A 4 -E "^Files:" | grep -E "^ " ' + 
                            ' | cut -d\  -f 2').read().split('\n')[:-1]

        for f in self.client.srcrecords.Files:
            self.failUnless(f[0] in src_hashes, "Couldn't find %s in: %r" % (f[0], src_hashes))

    def test_index_hash(self):
        self.client._load()

        indexhash = self.client.indexrecords[self.releaseFile[self.releaseFile.find('_dists_'):].replace('_','/')]['main/binary-i386/Packages.bz2']['SHA1'][0]

        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/binary-i386/Packages.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')

        self.failUnless(indexhash == idx_hash, "Hashes don't match: %s != %s" % (indexhash, idx_hash))

    def verifyHash(self, found_hash, path, true_hash):
        self.failUnless(found_hash.hexexpected() == true_hash, 
                    "%s hashes don't match: %s != %s" % (path, found_hash.hexexpected(), true_hash))

    def test_findIndexHash(self):
        lastDefer = defer.Deferred()
        
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/binary-i386/Packages.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = '/' + self.releaseFile[self.releaseFile.find('_dists_')+1:].replace('_','/')[:-7] + 'main/binary-i386/Packages.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

        d.addBoth(lastDefer.callback)
        return lastDefer

    def test_findPkgHash(self):
        lastDefer = defer.Deferred()
        
        pkg_hash = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^SHA1:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')
        pkg_path = '/' + os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^Filename:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')

        d = self.client.findHash(pkg_path)
        d.addCallback(self.verifyHash, pkg_path, pkg_hash)

        d.addBoth(lastDefer.callback)
        return lastDefer

    def test_findSrcHash(self):
        lastDefer = defer.Deferred()
        
        src_dir = '/' + os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -E "^Directory:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')
        src_hashes = os.popen('grep -A 20 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -A 4 -E "^Files:" | grep -E "^ " ' + 
                            ' | cut -d\  -f 2').read().split('\n')[:-1]
        src_paths = os.popen('grep -A 20 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -A 4 -E "^Files:" | grep -E "^ " ' + 
                            ' | cut -d\  -f 4').read().split('\n')[:-1]

        i = choice(range(len(src_hashes)))
        d = self.client.findHash(src_dir + '/' + src_paths[i])
        d.addCallback(self.verifyHash, src_dir + '/' + src_paths[i], src_hashes[i])
            
        d.addBoth(lastDefer.callback)
        return lastDefer

    def test_multipleFindHash(self):
        lastDefer = defer.Deferred()
        
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/binary-i386/Packages.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = '/' + self.releaseFile[self.releaseFile.find('_dists_')+1:].replace('_','/')[:-7] + 'main/binary-i386/Packages.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

        pkg_hash = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^SHA1:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')
        pkg_path = '/' + os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^Filename:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')

        d = self.client.findHash(pkg_path)
        d.addCallback(self.verifyHash, pkg_path, pkg_hash)

        src_dir = '/' + os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -E "^Directory:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')
        src_hashes = os.popen('grep -A 20 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -A 4 -E "^Files:" | grep -E "^ " ' + 
                            ' | cut -d\  -f 2').read().split('\n')[:-1]
        src_paths = os.popen('grep -A 20 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -A 4 -E "^Files:" | grep -E "^ " ' + 
                            ' | cut -d\  -f 4').read().split('\n')[:-1]

        for i in range(len(src_hashes)):
            d = self.client.findHash(src_dir + '/' + src_paths[i])
            d.addCallback(self.verifyHash, src_dir + '/' + src_paths[i], src_hashes[i])
            
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/source/Sources.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = '/' + self.releaseFile[self.releaseFile.find('_dists_')+1:].replace('_','/')[:-7] + 'main/source/Sources.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

        d.addBoth(lastDefer.callback)
        return lastDefer

    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.pending_calls = []
        self.client.cleanup()
        self.client = None
