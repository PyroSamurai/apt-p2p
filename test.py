import unittest

import ktable, khashmir
import hash, node, knode
import actions
import btemplate
import test_airhook
import test_krpc
tests = unittest.defaultTestLoader.loadTestsFromNames(['hash', 'node', 'knode', 'actions',  'ktable', 'test_airhook', 'test_krpc'])
result = unittest.TextTestRunner().run(tests)
