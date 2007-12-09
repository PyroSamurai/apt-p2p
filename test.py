## Copyright 2002-2003 Andrew Loewenstern, All Rights Reserved
# see LICENSE.txt for license information

import unittest

tests = unittest.defaultTestLoader.loadTestsFromNames(['hash', 'node', 'knode', 'actions',  'ktable', 'test_krpc'])
result = unittest.TextTestRunner().run(tests)
