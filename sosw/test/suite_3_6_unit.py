import unittest

from .test_app import app_UnitTestCase
from ..components.test.test_config import ConfigTestCase
from ..components.test.test_helpers import helpers_UnitTestCase
from ..components.test.test_siblings import siblings_TestCase
from ..components.test.test_sns import sns_TestCase


def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(app_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(helpers_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(siblings_TestCase))
    test_suite.addTest(unittest.makeSuite(sns_TestCase))
    test_suite.addTest(unittest.makeSuite(ConfigTestCase))

    return test_suite


mySuit = suite()

runner = unittest.TextTestRunner()
runner.run(mySuit)
