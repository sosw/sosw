import os
import unittest

from unittest.mock import patch

from sosw.essential import Essential
from sosw.test.variables import TEST_ESSENTIAL_CONFIG, TEST_ESSENTIAL_LABOURER_CONFIG


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Essential_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_ESSENTIAL_CONFIG
    LABOURERS = TEST_ESSENTIAL_LABOURER_CONFIG

    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()
        self.get_config_patch.return_value = self.LABOURERS
        self.essential = Essential(custom_config=self.TEST_CONFIG)


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass


    def test_init__updated_config(self):
        expected_config = self.TEST_CONFIG
        expected_config.update(self.LABOURERS)

        self.assertEqual(expected_config, self.essential.config)
