#
# Copyright (C) 2002 Manuel Estrada Sainz <ranty@debian.org>
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of version 2.1 of the GNU Lesser General Public
# License as published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

import warnings
warnings.simplefilter("ignore", FutureWarning)
import apt_pkg, apt_inst, sys, os, stat, random
from os.path import dirname, basename
import re, shelve, shutil, fcntl
from twisted.internet import process, threads, defer
from twisted.python import log
import copy, UserDict
from twisted.trial import unittest
from apt import OpProgress

aptpkg_dir='.apt-dht'
apt_pkg.init()

class AptDpkgInfo(UserDict.UserDict):
    """
    Gets control fields from a .deb file.

    And then behaves like a regular python dictionary.

    See AptPackages.get_mirror_path
    """

    def __init__(self, filename):
        UserDict.UserDict.__init__(self)
        try:
            filehandle = open(filename);
            try:
                self.control = apt_inst.debExtractControl(filehandle)
            finally:
                # Make sure that file is always closed.
                filehandle.close()
        except SystemError:
            log.msg("Had problems reading: %s"%(filename))
            raise
        for line in self.control.split('\n'):
            if line.find(': ') != -1:
                key, value = line.split(': ', 1)
                self.data[key] = value

class PackageFileList(UserDict.DictMixin):
    """
    Manages a list of package files belonging to a backend
    """
    def __init__(self, backendName, cache_dir):
        self.cache_dir = cache_dir
        self.packagedb_dir = cache_dir+'/'+ aptpkg_dir + \
                           '/backends/' + backendName
        if not os.path.exists(self.packagedb_dir):
            os.makedirs(self.packagedb_dir)
        self.packages = None
        self.open()

    def open(self):
        if self.packages is None:
            self.packages = shelve.open(self.packagedb_dir+'/packages.db')

    def close(self):
        if self.packages is not None:
            self.packages.close()

    def update_file(self, filename, cache_path, file_path):
        """
        Called from apt_proxy.py when files get updated so we can update our
        fake lists/ directory and sources.list.
        """
        if filename=="Packages" or filename=="Release" or filename=="Sources":
            log.msg("Registering package file: "+cache_path)
            self.packages[cache_path] = file_path
            return True
        return False

    def check_files(self):
        """
        Check all files in the database to make sure it exists.
        """
        files = self.packages.keys()
        #print self.packages.keys()
        for f in files:
            if not os.path.exists(self.packages[f]):
                log.msg("File in packages database has been deleted: "+f)
                del self.packages[f]
                
    def __getitem__(self, key): return self.packages[key]
    def __setitem__(self, key, item): self.packages[key] = item
    def __delitem__(self, key): del self.packages[key]
    def keys(self): return self.packages.keys()

class AptPackages:
    """
    Uses AptPackagesServer to answer queries about packages.

    Makes a fake configuration for python-apt for each backend.
    """
    DEFAULT_APT_CONFIG = {
        #'APT' : '',
        #'APT::Architecture' : 'amd64',  # TODO: Fix this, see bug #436011 and #285360
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
        
    def __init__(self, backendName, cache_dir):
        """
        Construct new packages manager
        backend: Name of backend associated with this packages file
        cache_dir: cache directory from config file
        """
        self.backendName = backendName
        self.cache_dir = cache_dir
        self.apt_config = copy.deepcopy(self.DEFAULT_APT_CONFIG)

        self.status_dir = (cache_dir+'/'+ aptpkg_dir
                           +'/backends/'+backendName)
        for dir in self.essential_dirs:
            path = self.status_dir+'/'+dir
            if not os.path.exists(path):
                os.makedirs(path)
        for file in self.essential_files:
            path = self.status_dir+'/'+file
            if not os.path.exists(path):
                f = open(path,'w')
                f.close()
                del f
                
        self.apt_config['Dir'] = self.status_dir
        self.apt_config['Dir::State::status'] = self.status_dir + '/apt/dpkg/status'
        #os.system('find '+self.status_dir+' -ls ')
        #print "status:"+self.apt_config['Dir::State::status']
        self.packages = PackageFileList(backendName, cache_dir)
        self.indexrecords = {}
        self.loaded = 0
        self.loading = None
        #print "Loaded aptPackages [%s] %s " % (self.backendName, self.cache_dir)
        
    def __del__(self):
        self.cleanup()
        #print "start aptPackages [%s] %s " % (self.backendName, self.cache_dir)
        self.packages.close()
        #print "Deleted aptPackages [%s] %s " % (self.backendName, self.cache_dir)
        
    def addRelease(self, cache_path, file_path):
        """
        Dirty hack until python-apt supports apt-pkg/indexrecords.h
        (see Bug #456141)
        """
        self.indexrecords[cache_path] = {}

        read_packages = False
        f = open(file_path, 'r')
        
        for line in f:
            line = line.rstrip()
    
            if line[:1] != " ":
                read_packages = False
                try:
                    # Read the various headers from the file
                    h, v = line.split(":", 1)
                    if h == "MD5Sum" or h == "SHA1" or h == "SHA256":
                        read_packages = True
                        hash_type = h
                except:
                    # Bad header line, just ignore it
                    log.msg("WARNING: Ignoring badly formatted Release line: %s" % line)
    
                # Skip to the next line
                continue
            
            # Read file names from the multiple hash sections of the file
            if read_packages:
                p = line.split()
                self.indexrecords[cache_path].setdefault(p[2], {})[hash_type] = (p[0], p[1])
        
        f.close()

    def file_updated(self, filename, cache_path, file_path):
        """
        A file in the backend has changed.  If this affects us, unload our apt database
        """
        if filename == "Release":
            self.addRelease(cache_path, file_path)
        if self.packages.update_file(filename, cache_path, file_path):
            self.unload()

    def load(self):
        if self.loading is None:
            self.loading = threads.deferToThread(self._load)
            self.loading.addCallback(self.doneLoading)
        return self.loading
        
    def doneLoading(self, loadResult):
        self.loading = None
        return loadResult
        
    def _load(self):
        """
        Regenerates the fake configuration and load the packages server.
        """
        if self.loaded: return True
        apt_pkg.InitSystem()
        #print "Load:", self.status_dir
        shutil.rmtree(self.status_dir+'/apt/lists/')
        os.makedirs(self.status_dir+'/apt/lists/partial')
        sources_filename = self.status_dir+'/'+'apt/etc/sources.list'
        sources = open(sources_filename, 'w')
        sources_count = 0
        self.packages.check_files()
        for f in self.packages:
            # we should probably clear old entries from self.packages and
            # take into account the recorded mtime as optimization
            filepath = self.packages[f]
            fake_uri='http://apt-dht/'+f
            if f.endswith('Sources'):
                source_line='deb-src '+dirname(fake_uri)+'/ /'
            else:
                source_line='deb '+dirname(fake_uri)+'/ /'
            listpath=(self.status_dir+'/apt/lists/'
                    +apt_pkg.URItoFileName(fake_uri))
            sources.write(source_line+'\n')
            log.msg("Sources line: " + source_line)
            sources_count = sources_count + 1

            try:
                #we should empty the directory instead
                os.unlink(listpath)
            except:
                pass
            os.symlink(self.packages[f], listpath)
        sources.close()

        if sources_count == 0:
            log.msg("No Packages files available for %s backend"%(self.backendName))
            return False

        log.msg("Loading Packages database for "+self.status_dir)
        #apt_pkg.Config = apt_pkg.newConfiguration(); #-- this causes unit tests to fail!
        for key, value in self.apt_config.items():
            apt_pkg.Config[key] = value
#         print "apt_pkg config:"
#         for I in apt_pkg.Config.keys():
#            print "%s \"%s\";"%(I,apt_pkg.Config[I]);

        self.cache = apt_pkg.GetCache(OpProgress())
        self.records = apt_pkg.GetPkgRecords(self.cache)
        self.srcrecords = apt_pkg.GetPkgSrcRecords()
        #for p in self.cache.Packages:
        #    print p
        #log.debug("%s packages found" % (len(self.cache)),'apt_pkg')
        self.loaded = 1
        return True

    def unload(self):
        "Tries to make the packages server quit."
        if self.loaded:
            del self.cache
            del self.records
            del self.srcrecords
            self.loaded = 0

    def cleanup(self):
        self.unload()
        self.packages.close()
        
    def findHash(self, path):
        d = defer.Deferred()

        for release in self.indexrecords:
            if path.startswith(release[:-7]):
                for indexFile in self.indexrecords[release]:
                    if release[:-7] + indexFile == path:
                        d.callback(self.indexrecords[release][indexFile]['SHA1'])
                        return d
        
        deferLoad = self.load()
        deferLoad.addCallback(self._findHash, path, d)
        
        return d

    def _findHash(self, loadResult, path, d):
        if not loadResult:
            d.callback((None, None))
            return loadResult
        
        package = path.split('/')[-1].split('_')[0]
        
        try:
            for version in self.cache[package].VersionList:
                size = version.Size
                for verFile in version.FileList:
                    if self.records.Lookup(verFile):
                        if self.records.FileName == path:
                            d.callback((self.records.SHA1Hash, size))
                            return loadResult
        except KeyError:
            pass
        
        self.srcrecords.Restart()
        if self.srcrecords.Lookup(package):
            for f in self.srcrecords.Files:
                if path == f[2]:
                    d.callback((f[0], f[1]))
                    return loadResult
        
        d.callback((None, None))
        return loadResult

    def get_mirror_path(self, name, version):
        "Find the path for version 'version' of package 'name'"
        if not self.load(): return None
        try:
            for pack_vers in self.cache[name].VersionList:
                if(pack_vers.VerStr == version):
                    file, index = pack_vers.FileList[0]
                    self.records.Lookup((file,index))
                    path = self.records.FileName
                    if len(path)>2 and path[0:2] == './': 
                        path = path[2:] # Remove any leading './'
                    return path

        except KeyError:
            pass
        return None
      

    def get_mirror_versions(self, package_name):
        """
        Find the available versions of the package name given
        @type package_name: string
        @param package_name: package name to search for e.g. ;apt'
        @return: A list of mirror versions available

        """
        vers = []
        if not self.load(): return vers
        try:
            for pack_vers in self.cache[package_name].VersionList:
                vers.append(pack_vers.VerStr)
        except KeyError:
            pass
        return vers


def cleanup(factory):
    for backend in factory.backends.values():
        backend.get_packages_db().cleanup()

def get_mirror_path(factory, file):
    """
    Look for the path of 'file' in all backends.
    """
    info = AptDpkgInfo(file)
    paths = []
    for backend in factory.backends.values():
        path = backend.get_packages_db().get_mirror_path(info['Package'],
                                                info['Version'])
        if path:
            paths.append('/'+backend.base+'/'+path)
    return paths

def get_mirror_versions(factory, package):
    """
    Look for the available version of a package in all backends, given
    an existing package name
    """
    all_vers = []
    for backend in factory.backends.values():
        vers = backend.get_packages_db().get_mirror_versions(package)
        for ver in vers:
            path = backend.get_packages_db().get_mirror_path(package, ver)
            all_vers.append((ver, "%s/%s"%(backend.base,path)))
    return all_vers

def closest_match(info, others):
    def compare(a, b):
        return apt_pkg.VersionCompare(a[0], b[0])

    others.sort(compare)
    version = info['Version']
    match = None
    for ver,path in others:
        if version <= ver:
            match = path
            break
    if not match:
        if not others:
            return None
        match = others[-1][1]

    dirname=re.sub(r'/[^/]*$', '', match)
    version=re.sub(r'^[^:]*:', '', info['Version'])
    if dirname.find('/pool/') != -1:
        return "/%s/%s_%s_%s.deb"%(dirname, info['Package'],
                                  version, info['Architecture'])
    else:
        return "/%s/%s_%s.deb"%(dirname, info['Package'], version)

def import_directory(factory, dir, recursive=0):
    """
    Import all files in a given directory into the cache
    This is used by apt-proxy-import to import new files
    into the cache
    """
    imported_count  = 0

    if not os.path.exists(dir):
        log.err('Directory ' + dir + ' does not exist')
        return

    if recursive:    
        log.msg("Importing packages from directory tree: " + dir)
        for root, dirs, files in os.walk(dir):
            for file in files:
                imported_count += import_file(factory, root, file)
    else:
        log.msg("Importing packages from directory: " + dir)
        for file in os.listdir(dir):
            mode = os.stat(dir + '/' + file)[stat.ST_MODE]
            if not stat.S_ISDIR(mode):
                imported_count += import_file(factory, dir, file)

    for backend in factory.backends.values():
        backend.get_packages_db().unload()

    log.msg("Imported %s files" % (imported_count))
    return imported_count

def import_file(factory, dir, file):
    """
    Import a .deb or .udeb into cache from given filename
    """
    if file[-4:]!='.deb' and file[-5:]!='.udeb':
        log.msg("Ignoring (unknown file type):"+ file)
        return 0
    
    log.msg("considering: " + dir + '/' + file)
    try:
        paths = get_mirror_path(factory, dir+'/'+file)
    except SystemError:
        log.msg(file + ' skipped - wrong format or corrupted')
        return 0
    if paths:
        if len(paths) != 1:
            log.msg("WARNING: multiple ocurrences")
            log.msg(str(paths), 'import')
        cache_path = paths[0]
    else:
        log.msg("Not found, trying to guess")
        info = AptDpkgInfo(dir+'/'+file)
        cache_path = closest_match(info,
                                get_mirror_versions(factory, info['Package']))
    if cache_path:
        log.msg("MIRROR_PATH:"+ cache_path)
        src_path = dir+'/'+file
        dest_path = factory.config.cache_dir+cache_path
        
        if not os.path.exists(dest_path):
            log.msg("IMPORTING:" + src_path)
            dest_path = re.sub(r'/\./', '/', dest_path)
            if not os.path.exists(dirname(dest_path)):
                os.makedirs(dirname(dest_path))
            f = open(dest_path, 'w')
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX)
            f.truncate(0)
            shutil.copy2(src_path, dest_path)
            f.close()
            if hasattr(factory, 'access_times'):
                atime = os.stat(src_path)[stat.ST_ATIME]
                factory.access_times[cache_path] = atime
            log.msg(file + ' imported')
            return 1
        else:
            log.msg(file + ' skipped - already in cache')
            return 0

    else:
        log.msg(file + ' skipped - no suitable backend found')
        return 0
            
class TestAptPackages(unittest.TestCase):
    """Unit tests for the AptPackages cache."""
    
    pending_calls = []
    client = None
    packagesFile = ''
    sourcesFile = ''
    releaseFile = ''
    
    def setUp(self):
        self.client = AptPackages('whatever', '/tmp')
    
        self.packagesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "Packages$" | tail -n 1').read().rstrip('\n')
        self.sourcesFile = os.popen('ls -Sr /var/lib/apt/lists/ | grep -E "Sources$" | tail -n 1').read().rstrip('\n')
        for f in os.walk('/var/lib/apt/lists').next()[2]:
            if f[-7:] == "Release" and self.packagesFile.startswith(f[:-7]):
                self.releaseFile = f
                break
        
        self.client.file_updated('Release', 
                                 self.releaseFile[self.releaseFile.find('_debian_')+1:].replace('_','/'), 
                                 '/var/lib/apt/lists/' + self.releaseFile)
        self.client.file_updated('Packages', 
                                 self.packagesFile[self.packagesFile.find('_debian_')+1:].replace('_','/'), 
                                 '/var/lib/apt/lists/' + self.packagesFile)
        self.client.file_updated('Sources', 
                                 self.sourcesFile[self.sourcesFile.find('_debian_')+1:].replace('_','/'), 
                                 '/var/lib/apt/lists/' + self.sourcesFile)
    
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

        indexhash = self.client.indexrecords[self.releaseFile[self.releaseFile.find('_debian_')+1:].replace('_','/')]['main/binary-i386/Packages.bz2']['SHA1'][0]

        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/binary-i386/Packages.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')

        self.failUnless(indexhash == idx_hash, "Hashes don't match: %s != %s" % (indexhash, idx_hash))

    def verifyHash(self, found_hash, path, true_hash):
        self.failUnless(found_hash[0] == true_hash, 
                    "%s hashes don't match: %s != %s" % (path, found_hash[0], true_hash))

    def test_findIndexHash(self):
        lastDefer = defer.Deferred()
        
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/binary-i386/Packages.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = self.releaseFile[self.releaseFile.find('_debian_')+1:].replace('_','/')[:-7] + 'main/binary-i386/Packages.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

        d.addCallback(lastDefer.callback)
        return lastDefer

    def test_findPkgHash(self):
        lastDefer = defer.Deferred()
        
        pkg_hash = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^SHA1:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')
        pkg_path = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^Filename:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')

        d = self.client.findHash(pkg_path)
        d.addCallback(self.verifyHash, pkg_path, pkg_hash)

        d.addCallback(lastDefer.callback)
        return lastDefer

    def test_findSrcHash(self):
        lastDefer = defer.Deferred()
        
        src_dir = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
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

        i = random.choice(range(len(src_hashes)))
        d = self.client.findHash(src_dir + '/' + src_paths[i])
        d.addCallback(self.verifyHash, src_dir + '/' + src_paths[i], src_hashes[i])
            
        d.addCallback(lastDefer.callback)
        return lastDefer

    def test_multipleFindHash(self):
        lastDefer = defer.Deferred()
        
        idx_hash = os.popen('grep -A 3000 -E "^SHA1:" ' + 
                            '/var/lib/apt/lists/' + self.releaseFile + 
                            ' | grep -E " main/binary-i386/Packages.bz2$"'
                            ' | head -n 1 | cut -d\  -f 2').read().rstrip('\n')
        idx_path = self.releaseFile[self.releaseFile.find('_debian_')+1:].replace('_','/')[:-7] + 'main/binary-i386/Packages.bz2'

        d = self.client.findHash(idx_path)
        d.addCallback(self.verifyHash, idx_path, idx_hash)

        pkg_hash = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^SHA1:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')
        pkg_path = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
                            '/var/lib/apt/lists/' + self.packagesFile + 
                            ' | grep -E "^Filename:" | head -n 1' + 
                            ' | cut -d\  -f 2').read().rstrip('\n')

        d = self.client.findHash(pkg_path)
        d.addCallback(self.verifyHash, pkg_path, pkg_hash)

        src_dir = os.popen('grep -A 30 -E "^Package: dpkg$" ' + 
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
            
        d.addCallback(lastDefer.callback)
        return lastDefer

    def tearDown(self):
        for p in self.pending_calls:
            if p.active():
                p.cancel()
        self.pending_calls = []
        self.client.cleanup()
        self.client = None
