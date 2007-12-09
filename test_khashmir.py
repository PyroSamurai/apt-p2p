from unittest import *
from khashmir import *
import khash
from copy import copy

from random import randrange

import os

if __name__ =="__main__":
    tests = defaultTestLoader.loadTestsFromNames([sys.argv[0][:-3]])
    result = TextTestRunner().run(tests)

class SimpleTests(TestCase):
    def setUp(self):
        self.a = Khashmir('127.0.0.1', 4044, '/tmp/a.test')
        self.b = Khashmir('127.0.0.1', 4045, '/tmp/b.test')
        
    def tearDown(self):
        self.a.listenport.stopListening()
        self.b.listenport.stopListening()
        os.unlink('/tmp/a.test')
        os.unlink('/tmp/b.test')                
        reactor.iterate()
        reactor.iterate()

    def addContacts(self):
        self.a.addContact('127.0.0.1', 4045)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()

    def testAddContact(self):
        self.assertEqual(len(self.a.table.buckets), 1) 
        self.assertEqual(len(self.a.table.buckets[0].l), 0)

        self.assertEqual(len(self.b.table.buckets), 1) 
        self.assertEqual(len(self.b.table.buckets[0].l), 0)

        self.addContacts()

        self.assertEqual(len(self.a.table.buckets), 1) 
        self.assertEqual(len(self.a.table.buckets[0].l), 1)
        self.assertEqual(len(self.b.table.buckets), 1) 
        self.assertEqual(len(self.b.table.buckets[0].l), 1)

    def testStoreRetrieve(self):
        self.addContacts()
        self.got = 0
        self.a.storeValueForKey(sha('foo').digest(), 'foobar')
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        self.a.valueForKey(sha('foo').digest(), self._cb)
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()
        reactor.iterate()

    def _cb(self, val):
        if not val:
            self.assertEqual(self.got, 1)
        elif 'foobar' in val:
            self.got = 1


class MultiTest(TestCase):
    num = 20
    def _done(self, val):
        self.done = 1
        
    def setUp(self):
        self.l = []
        self.startport = 4088
        for i in range(self.num):
            self.l.append(Khashmir('127.0.0.1', self.startport + i, '/tmp/%s.test' % (self.startport + i)))
        reactor.iterate()
        reactor.iterate()
        
        for i in self.l:
            i.addContact('127.0.0.1', self.l[randrange(0,self.num)].port)
            i.addContact('127.0.0.1', self.l[randrange(0,self.num)].port)
            i.addContact('127.0.0.1', self.l[randrange(0,self.num)].port)
            reactor.iterate()
            reactor.iterate()
            reactor.iterate() 
            
        for i in self.l:
            self.done = 0
            i.findCloseNodes(self._done)
            while not self.done:
                reactor.iterate()
        for i in self.l:
            self.done = 0
            i.findCloseNodes(self._done)
            while not self.done:
                reactor.iterate()

    def tearDown(self):
        for i in self.l:
            i.listenport.stopListening()
        for i in range(self.startport, self.startport+self.num):
            os.unlink('/tmp/%s.test' % i)
            
        reactor.iterate()
        
    def testStoreRetrieve(self):
        for i in range(10):
            K = khash.newID()
            V = khash.newID()
            
            for a in range(3):
                self.done = 0
                def _scb(val):
                    self.done = 1
                self.l[randrange(0, self.num)].storeValueForKey(K, V, _scb)
                while not self.done:
                    reactor.iterate()


                def _rcb(val):
                    if not val:
                        self.done = 1
                        self.assertEqual(self.got, 1)
                    elif V in val:
                        self.got = 1
                for x in range(3):
                    self.got = 0
                    self.done = 0
                    self.l[randrange(0, self.num)].valueForKey(K, _rcb)
                    while not self.done:
                        reactor.iterate()


            
            
            
