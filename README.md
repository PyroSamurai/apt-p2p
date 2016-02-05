Apt-P2P: a peer-to-peer proxy for apt downloads

### Goal

Similar to [DebTorrent](http://debtorrent.alioth.debian.org/), Apt-P2P will act as a proxy between apt
requests and a debian repository server, downloading any requested
files from peers (if possible), but falling back to a direct HTTP
download. Unlike DebTorrent, Apt-P2P will be simple, efficient, and
fast.

 
### Features

* Downloads from peers, increasing the available bandwidth to the 
  user
* Reduces the bandwidth requirements needed to setup a repository 
  of packages
* Seamlessly integrates with the current APT tool
* Automatically falls back to downloading from an HTTP mirror when 
  peers are not available
* Builds on other already existing tools where possible
* Fast and requires limited CPU and memory
* Will try to download any file it can find a hash for from peers
  (including Packages.bz2, Sources.gz, ...)

This free/libre software and is released at no charge under the 
terms of the [GPLv2 license](https://gnu.org/licenses/gpl-2.0.html).


### Requirements

To run the full program (e.g. to actually download something):

* Python 2.4 or higher
* [Twisted](http://twistedmatrix.com/trac/) 2.4 or higher
  - including [Twisted Web2](http://twistedmatrix.com/trac/wiki/TwistedWeb2) 0.2 or higher
* [python-apt](http://packages.debian.org/unstable/python/python-apt) 0.6.20 or higher
* An APT-based package management system (such as Debian distributions 
  have)

If you just want to run the DHT part of the program (e.g. to provide a
bootstrap node), then you only need:

* Python 2.4 or higher
* [Twisted](http://twistedmatrix.com/trac/) 2.4 or higher


### Development Status

This project is still undergoing massive code and protocol changes
and hasn't yet seen a release. The code has worked, and is
occasionally working, and will definitely be working soon in the
future.

### Installing

There are detailed instructions on the [website](http://www.camrdale.org/apt-p2p.html) showing how to
install the Apt-P2P program.

### The Code

It is originally based on the [khashmir](http://khashmir.sourceforge.net/) implementation
of the [kademlia DHT](http://en.wikipedia.org/wiki/Kademlia). All of the networking
is handled by the Twisted and Twisted Web2 libraries. 
Dealing with apt's repository files is handled by
python-apt, the code for which is based on that of the
[apt-proxy](http://apt-proxy.sourceforge.net/) program.

