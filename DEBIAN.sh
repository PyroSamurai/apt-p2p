# How To Install Khashmir on Debian

cd ..
sudo apt-get install python2.2
wget http://us.dl.sourceforge.net/twisted/Twisted-0.99.1rc4.tar.gz
wget http://us.dl.sf.net/pybsddb/bsddb3-3.4.0.tar.gz
tar xzf Twisted-0.99.1rc4.tar.gz
cd Twisted-0.99.1rc4
sudo python2.2 setup.py install
cd ..
tar xzf bsddb3-3.4.0.tar.gz
cd bsddb3-3.4.0
sudo python2.2 setup.py install --berkeley-db=/usr
cd ../khashmir
python2.2 test.py