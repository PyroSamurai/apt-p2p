import unittest

import ktable, khashmir
import hash, node, knode
import actions, xmlrpcclient
import btemplate

tests = unittest.defaultTestLoader.loadTestsFromNames(['hash', 'node', 'knode', 'btemplate', 'actions',  'ktable', 'xmlrpcclient'])
result = unittest.TextTestRunner().run(tests)
