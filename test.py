import unittest

import hash, node, messages
import listener, dispatcher
import ktable, transactions, khashmir

import bencode, btemplate

tests = unittest.defaultTestLoader.loadTestsFromNames(['hash', 'node', 'bencode', 'btemplate', 'listener', 'messages', 'dispatcher', 'transactions', 'ktable'])
result = unittest.TextTestRunner().run(tests)
