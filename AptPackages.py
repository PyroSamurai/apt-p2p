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

import apt_pkg, apt_inst, sys, os, stat
from os.path import dirname, basename
import re, shelve, shutil, fcntl
from twisted.internet import process
from twisted.python import log
import copy, UserDict

aptpkg_dir='.apt-dht'
apt_pkg.InitSystem()

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

class PackageFileList:
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

    def update_file(self, entry):
        """
        Called from apt_proxy.py when files get updated so we can update our
        fake lists/ directory and sources.list.

        @param entry CacheEntry for cached file
        """
        if entry.filename=="Packages" or entry.filename=="Release":
            log.msg("Registering package file: "+entry.cache_path)
            stat_result = os.stat(entry.file_path)
            self.packages[entry.cache_path] = stat_result

    def get_files(self):
        """
        Get list of files in database.  Each file will be checked that it exists
        """
        files = self.packages.keys()
        #print self.packages.keys()
        for f in files:
            if not os.path.exists(self.cache_dir + os.sep + f):
                log.msg("File in packages database has been deleted: "+f)
                del files[files.index(f)]
                del self.packages[f]
        return files

class AptPackages:
    """
    Uses AptPackagesServer to answer queries about packages.

    Makes a fake configuration for python-apt for each backend.
    """
    DEFAULT_APT_CONFIG = {
        #'APT' : '',
	'APT::Architecture' : 'i386',  # TODO: Fix this, see bug #436011 and #285360
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
        self.loaded = 0
        #print "Loaded aptPackages [%s] %s " % (self.backendName, self.cache_dir)
        
    def __del__(self):
        self.cleanup()
        #print "start aptPackages [%s] %s " % (self.backendName, self.cache_dir)
        self.packages.close()
        #print "Deleted aptPackages [%s] %s " % (self.backendName, self.cache_dir)
    def file_updated(self, entry):
        """
        A file in the backend has changed.  If this affects us, unload our apt database
        """
        if self.packages.update_file(entry):
            self.unload()

    def __save_stdout(self):
        self.real_stdout_fd = os.dup(1)
        os.close(1)
                
    def __restore_stdout(self):
        os.dup2(self.real_stdout_fd, 1)
        os.close(self.real_stdout_fd)
        del self.real_stdout_fd

    def load(self):
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
        for file in self.packages.get_files():
            # we should probably clear old entries from self.packages and
            # take into account the recorded mtime as optimization
            filepath = self.cache_dir + file
            fake_uri='http://apt-dht/'+file
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
            os.symlink('../../../../../'+file, listpath)
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

        # apt_pkg prints progress messages to stdout, disable
        self.__save_stdout()
        try:
            self.cache = apt_pkg.GetCache()
        finally:
            pass
            self.__restore_stdout()

        self.records = apt_pkg.GetPkgRecords(self.cache)
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
            self.loaded = 0

    def cleanup(self):
        self.unload()

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
            
def test(factory, file):
    "Just for testing purposes, this should probably go to hell soon."
    for backend in factory.backends:
        backend.get_packages_db().load()

    info = AptDpkgInfo(file)
    path = get_mirror_path(factory, file)
    print "Exact Match:"
    print "\t%s:%s"%(info['Version'], path)

    vers = get_mirror_versions(factory, info['Package'])
    print "Other Versions:"
    for ver in vers:
        print "\t%s:%s"%(ver)
    print "Guess:"
    print "\t%s:%s"%(info['Version'], closest_match(info, vers))

if __name__ == '__main__':
    from apt_proxy_conf import factoryConfig
    class DummyFactory:
        def debug(self, msg):
            pass
    factory = DummyFactory()
    factoryConfig(factory)
    test(factory,
         '/home/ranty/work/apt-proxy/related/tools/galeon_1.2.5-1_i386.deb')
    test(factory,
         '/storage/apt-proxy/debian/dists/potato/main/binary-i386/base/'
         +'libstdc++2.10_2.95.2-13.deb')

    cleanup(factory)

