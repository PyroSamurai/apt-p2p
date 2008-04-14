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

"""Manage a mirror's index files.

@type TRACKED_FILES: C{list} of C{string}
@var TRACKED_FILES: the file names of files that contain index information
"""

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

from apt_p2p_conf import config
from Hash import HashObject

apt_pkg.init()

TRACKED_FILES = ['release', 'sources', 'packages']

class PackageFileList(DictMixin):
    """Manages a list of index files belonging to a mirror.
    
    @type cache_dir: L{twisted.python.filepath.FilePath}
    @ivar cache_dir: the directory to use for storing all files
    @type packages: C{shelve dictionary}
    @ivar packages: the files tracked for this mirror
    """
    
    def __init__(self, cache_dir):
        """Initialize the list by opening the dictionary."""
        self.cache_dir = cache_dir
        self.cache_dir.restat(False)
        if not self.cache_dir.exists():
            self.cache_dir.makedirs()
        self.packages = None
        self.open()

    def open(self):
        """Open the persistent dictionary of files for this mirror."""
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
        
        @type cache_path: C{string}
        @param cache_path: the location of the file within the mirror
        @type file_path: L{twisted.python.filepath.FilePath}
        @param file_path: The location of the file in the file system
        @rtype: C{boolean}
        @return: whether the file is an index file
        """
        filename = cache_path.split('/')[-1]
        if filename.lower() in TRACKED_FILES:
            log.msg("Registering package file: "+cache_path)
            self.packages[cache_path] = file_path
            return True
        return False

    def check_files(self):
        """Check all files in the database to remove any that don't exist."""
        files = self.packages.keys()
        for f in files:
            self.packages[f].restat(False)
            if not self.packages[f].exists():
                log.msg("File in packages database has been deleted: "+f)
                del self.packages[f]

    #{ Dictionary interface details
    def __getitem__(self, key): return self.packages[key]
    def __setitem__(self, key, item): self.packages[key] = item
    def __delitem__(self, key): del self.packages[key]
    def keys(self): return self.packages.keys()

class AptPackages:
    """Answers queries about packages available from a mirror.
    
    Uses the python-apt tools to parse and provide information about the
    files that are available on a single mirror.
    
    @ivar DEFAULT_APT_CONFIG: the default configuration parameters to use for apt
    @ivar essential_dirs: directories that must be created for apt to work
    @ivar essential_files: files that must be created for apt to work
    @type cache_dir: L{twisted.python.filepath.FilePath}
    @ivar cache_dir: the directory to use for storing all files
    @ivar apt_config: the configuration parameters to use for apt
    @type packages: L{PackageFileList}
    @ivar packages: the persistent storage of tracked apt index files
    @type loaded: C{boolean}
    @ivar loaded: whether the apt cache is currently loaded
    @type loading: L{twisted.internet.defer.Deferred}
    @ivar loading: if the cache is currently being loaded, this will be
        called when it is loaded, otherwise it is None
    @type unload_later: L{twisted.internet.interfaces.IDelayedCall}
    @ivar unload_later: the delayed call to unload the apt cache
    @type indexrecords: C{dictionary}
    @ivar indexrecords: the hashes of index files for the mirror, keys are
        mirror directories, values are dictionaries with keys the path to the
        index file in the mirror directory and values are dictionaries with
        keys the hash type and values the hash
    @type cache: C{apt_pkg.GetCache()}
    @ivar cache: the apt cache of the mirror
    @type records: C{apt_pkg.GetPkgRecords()}
    @ivar records: the apt package records for all binary packages in a mirror
    @type srcrecords: C{apt_pkg.GetPkgSrcRecords}
    @ivar srcrecords: the apt package records for all source packages in a mirror
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
        
    def __init__(self, cache_dir):
        """Construct a new packages manager.

        @param cache_dir: directory to use to store files for this mirror
        """
        self.cache_dir = cache_dir
        self.apt_config = deepcopy(self.DEFAULT_APT_CONFIG)

        # Create the necessary files and directories for apt
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
        self.loaded = False
        self.loading = None
        self.unload_later = None
        
    def __del__(self):
        self.cleanup()
        
    def addRelease(self, cache_path, file_path):
        """Add a Release file's info to the list of index files.
        
        Dirty hack until python-apt supports apt-pkg/indexrecords.h
        (see Bug #456141)
        """
        self.indexrecords[cache_path] = {}

        read_packages = False
        f = file_path.open('r')
        
        # Use python-debian routines to parse the file for hashes
        rel = deb822.Release(f, fields = ['MD5Sum', 'SHA1', 'SHA256'])
        for hash_type in rel:
            for file in rel[hash_type]:
                self.indexrecords[cache_path].setdefault(file['name'], {})[hash_type.upper()] = (file[hash_type], file['size'])
            
        f.close()

    def file_updated(self, cache_path, file_path):
        """A file in the mirror has changed or been added.
        
        If this affects us, unload our apt database.
        @see: L{PackageFileList.update_file}
        """
        if self.packages.update_file(cache_path, file_path):
            self.unload()

    def load(self):
        """Make sure the package cache is initialized and loaded."""
        # Reset the pending unload call
        if self.unload_later and self.unload_later.active():
            self.unload_later.reset(config.gettime('DEFAULT', 'UNLOAD_PACKAGES_CACHE'))
        else:
            self.unload_later = reactor.callLater(config.gettime('DEFAULT', 'UNLOAD_PACKAGES_CACHE'), self.unload)
            
        # Make sure it's not already being loaded
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
        """Regenerates the fake configuration and loads the packages caches."""
        if self.loaded: return True
        
        # Modify the default configuration to create the fake one.
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
        
        # Create an entry in sources.list for each needed index file
        for f in self.packages:
            # we should probably clear old entries from self.packages and
            # take into account the recorded mtime as optimization
            file = self.packages[f]
            if f.split('/')[-1] == "Release":
                self.addRelease(f, file)
            fake_uri='http://apt-p2p'+f
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

        self.loaded = True
        return True

    def unload(self):
        """Tries to make the packages server quit."""
        if self.unload_later and self.unload_later.active():
            self.unload_later.cancel()
        self.unload_later = None
        if self.loaded:
            log.msg('Unloading the packages cache')
            # This should save memory
            del self.cache
            del self.records
            del self.srcrecords
            del self.indexrecords
            self.loaded = False

    def cleanup(self):
        """Cleanup and close any loaded caches."""
        self.unload()
        if self.unload_later and self.unload_later.active():
            self.unload_later.cancel()
        self.packages.close()
        
    def findHash(self, path):
        """Find the hash for a given path in this mirror.
        
        @type path: C{string}
        @param path: the path within the mirror of the file to lookup
        @rtype: L{twisted.internet.defer.Deferred}
        @return: a deferred so it can make sure the cache is loaded first
        """
        d = defer.Deferred()

        deferLoad = self.load()
        deferLoad.addCallback(self._findHash, path, d)
        deferLoad.addErrback(self._findHash_error, path, d)
        
        return d

    def _findHash_error(self, failure, path, d):
        """An error occurred, return an empty hash."""
        log.msg('An error occurred while looking up a hash for: %s' % path)
        log.err(failure)
        d.callback(HashObject())
        return failure

    def _findHash(self, loadResult, path, d):
        """Search the records for the hash of a path.
        
        @type loadResult: C{boolean}
        @param loadResult: whether apt's cache was successfully loaded
        @type path: C{string}
        @param path: the path within the mirror of the file to lookup
        @type d: L{twisted.internet.defer.Deferred}
        @param d: the deferred to callback with the result
        """
        if not loadResult:
            d.callback(HashObject())
            return loadResult
        
        h = HashObject()
        
        # First look for the path in the cache of index files
        for release in self.indexrecords:
            if path.startswith(release[:-7]):
                for indexFile in self.indexrecords[release]:
                    if release[:-7] + indexFile == path:
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
                        h.setFromSrcRecord(f)
                        d.callback(h)
                        return loadResult
        
        d.callback(h)
        
        # Have to pass the returned loadResult on in case other calls to this function are pending.
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
        """Initializes the cache with files found in the traditional apt location."""
        self.client = AptPackages(FilePath('/tmp/.apt-p2p'))
    
        # Find the largest index files that are for 'main'
        self.packagesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "_main_.*Packages$" | tail -n 1').read().rstrip('\n')
        self.sourcesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "_main_.*Sources$" | tail -n 1').read().rstrip('\n')
        
        # Find the Release file corresponding to the found Packages file
        for f in os.walk('/var/lib/apt/lists').next()[2]:
            if f[-7:] == "Release" and self.packagesFile.startswith(f[:-7]):
                self.releaseFile = f
                break

        # Add all the found files to the PackageFileList
        self.client.file_updated(self.releaseFile[self.releaseFile.find('_dists_'):].replace('_','/'), 
                                 FilePath('/var/lib/apt/lists/' + self.releaseFile))
        self.client.file_updated(self.packagesFile[self.packagesFile.find('_dists_'):].replace('_','/'), 
                                 FilePath('/var/lib/apt/lists/' + self.packagesFile))
        self.client.file_updated(self.sourcesFile[self.sourcesFile.find('_dists_'):].replace('_','/'), 
                                 FilePath('/var/lib/apt/lists/' + self.sourcesFile))
    
    def test_pkg_hash(self):
        """Tests loading the binary package records cache."""
        self.client._load()

        self.client.records.Lookup(self.client.cache['dpkg'].VersionList[0].FileList[0])
        
        pkg_hash = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^SHA1:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')

        self.failUnless(self.client.records.SHA1Hash == pkg_hash, 
                        "Hashes don't match: %s != %s" % (self.client.records.SHA1Hash, pkg_hash))

    def test_src_hash(self):
        """Tests loading the source package records cache."""
        self.client._load()

        self.client.srcrecords.Lookup('dpkg')

        src_hashes = os.popen('grep -A 20 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.sourcesFile + 
                            ' | grep -A 4 -E "^Files:" | grep -E "^ " ' + 
                            ' | cut -d\  -f 2').read().split('\n')[:-1]

        for f in self.client.srcrecords.Files:
            self.failUnless(f[0] in src_hashes, "Couldn't find %s in: %r" % (f[0], src_hashes))

    def test_index_hash(self):
        """Tests loading the cache of index file information."""
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
        """Tests finding the hash of a single index file."""
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
        """Tests finding the hash of a single binary package."""
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
        """Tests finding the hash of a single source package."""
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
        """Tests finding the hash of an index file, binary package, source package, and another index file."""
        lastDefer = defer.Deferred()
        
        # Lookup a Packages.bz2 file
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/binary-i386/Packages.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = '/' + self.releaseFile[self.releaseFile.find('_dists_')+1:].replace('_','/')[:-7] + 'main/binary-i386/Packages.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

        # Lookup the binary 'dpkg' package
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

        # Lookup the source 'dpkg' package
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
            
        # Lookup a Sources.bz2 file
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
