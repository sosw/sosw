import boto3
import os
import unittest

from unittest import mock
from unittest.mock import MagicMock, patch

from sosw.orchestrator import Orchestrator
from sosw.test.variables import TEST_CONFIG


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Orchestrator_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_CONFIG


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.custom_config = self.TEST_CONFIG.copy()
        self.orchestrator = Orchestrator(self.custom_config)


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except:
            pass


    def test_true(self):
        self.assertEqual(1, 1)


    def test_get_worker_setting(self):

        custom_config = self.TEST_CONFIG.copy()
        custom_config['workers'] = {
            42: {'foo': 'bar'},
        }
        orchestrator = Orchestrator(custom_config)

        self.assertEqual(orchestrator.get_worker_setting(42, 'foo'), 'bar')
        self.assertEqual(orchestrator.get_worker_setting(42, 'faz'), None)

        self.assertEqual(orchestrator.get_worker_setting(4422, 'foo'), None)
        self.assertEqual(orchestrator.get_worker_setting(4422, 'faz'), None)
