import unittest

import hash, node, messages
import ktable, transactions, khashmir
import protocol
import bencode, btemplate

tests = unittest.defaultTestLoader.loadTestsFromNames(['hash', 'node', 'bencode', 'btemplate', 'messages', 'transactions', 'ktable', 'protocol'])
result = unittest.TextTestRunner().run(tests)
