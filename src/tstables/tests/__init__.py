from tstables.tests import test_tstable_static
from tstables.tests import test_tstable_file
#from tstables import tstable

def suite():
    import unittest
    import doctest
    suite = unittest.TestSuite()
    #suite.addTests(doctest.DocTestSuite(tstable))
    suite.addTests(test_tstable_static.suite())
    suite.addTests(test_tstable_file.suite())
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())