import unittest

import ktable, khashmir
import hash, node, knode
import actions, xmlrpcclient
import bencode, btemplate

tests = unittest.defaultTestLoader.loadTestsFromNames(['hash', 'node', 'knode', 'bencode', 'btemplate', 'actions',  'ktable', 'xmlrpcclient'])
result = unittest.TextTestRunner().run(tests)
