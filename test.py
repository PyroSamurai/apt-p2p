import unittest

import ktable, khashmir
import hash, node, knode
import actions
import btemplate
import test_airhook

tests = unittest.defaultTestLoader.loadTestsFromNames(['hash', 'node', 'knode', 'actions',  'ktable', 'test_airhook'])
result = unittest.TextTestRunner().run(tests)
