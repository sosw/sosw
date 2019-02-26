import boto3
import os
import unittest

from unittest import mock
from unittest.mock import MagicMock, patch

from sosw.orchestrator import Orchestrator


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Orchestrator_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = {'test': True}


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except:
            pass


    def test_true(self):
        self.assertEqual(1, 1)
