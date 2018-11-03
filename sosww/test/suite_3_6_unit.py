import unittest

from .test_app import app_TestCase
from ..components.test.test_helpers import helpers_TestCase
from ..components.test.test_siblings import siblings_TestCase


def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(app_TestCase))
    test_suite.addTest(unittest.makeSuite(helpers_TestCase))
    test_suite.addTest(unittest.makeSuite(siblings_TestCase))
    return test_suite


mySuit = suite()

runner = unittest.TextTestRunner()
runner.run(mySuit)
