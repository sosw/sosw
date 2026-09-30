import unittest

# Core applications
from .unit.test_app import app_UnitTestCase
from .unit.test_durable import durable_UnitTestCase, durable_WithFakeSdk_UnitTestCase
from .unit.test_lambda_api import lambda_api_UnitTestCase
from .unit.test_powertools_guards import powertools_guards_UnitTestCase
from .unit.test_sosw_init import sosw_init_UnitTestCase

# Components
from ..components.test.unit.test_config import Config_UnitTestCase, DynamoConfig_UnitTestCase, SSMConfig_UnitTestCase
from ..components.test.unit.test_decorators import decorators_UnitTestCase
from ..components.test.unit.test_dynamo_db import dynamodb_client_UnitTestCase
from ..components.test.unit.test_dynamo_db_init import dynamodb_client_init_UnitTestCase
from ..components.test.unit.test_helpers import helpers_UnitTestCase
from ..components.test.unit.test_secrets_manager import secretsmanager_client_UnitTestCase
from sosw.components.test.unit.test_siblings import siblings_TestCase
from sosw.components.test.unit.test_sns import sns_TestCase
from sosw.components.test.unit.test_sigv4 import sigv4_TestCase


def suite():
    test_suite = unittest.TestSuite()

    # Core applications
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(app_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(durable_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(durable_WithFakeSdk_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(lambda_api_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(powertools_guards_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(sosw_init_UnitTestCase))

    # Components
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(Config_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(DynamoConfig_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(SSMConfig_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(decorators_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(dynamodb_client_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(dynamodb_client_init_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(helpers_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(secretsmanager_client_UnitTestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(siblings_TestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(sns_TestCase))
    test_suite.addTest(unittest.TestLoader().loadTestsFromTestCase(sigv4_TestCase))

    return test_suite


if __name__ == '__main__':
    mySuit = suite()

    runner = unittest.TextTestRunner()
    runner.run(mySuit)
