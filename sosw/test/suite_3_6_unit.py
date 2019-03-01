import unittest

from .unit.test_app import app_UnitTestCase
from .unit.test_labourer import Labourer_UnitTestCase
from .unit.test_orchestrator import Orchestrator_UnitTestCase
from ..components.test.test_config import ConfigTestCase
from ..components.test.unit.test_helpers import helpers_UnitTestCase
from ..components.test.unit.test_dynamo_db import dynamodb_client_UnitTestCase
from ..components.test.test_siblings import siblings_TestCase
from ..components.test.test_sns import sns_TestCase

from ..managers.test.unit.test_task import *
from ..managers.test.unit.test_ecology import *

def suite():
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(app_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(Labourer_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(Orchestrator_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(helpers_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(dynamodb_client_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(siblings_TestCase))
    test_suite.addTest(unittest.makeSuite(sns_TestCase))
    test_suite.addTest(unittest.makeSuite(ConfigTestCase))

    test_suite.addTest(unittest.makeSuite(ecology_manager_UnitTestCase))
    test_suite.addTest(unittest.makeSuite(task_manager_UnitTestCase))

    return test_suite


if __name__ == '__main__':
    mySuit = suite()

    runner = unittest.TextTestRunner()
    runner.run(mySuit)
