import boto3
import os
import unittest

from unittest import mock
from unittest.mock import MagicMock, patch

from sosw.orchestrator import Orchestrator
from sosw.labourer import Labourer
from sosw.test.variables import TEST_ORCHESTRATOR_CONFIG


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Orchestrator_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_ORCHESTRATOR_CONFIG


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


    def test_get_labourer_setting(self):

        custom_config = self.TEST_CONFIG.copy()
        custom_config['labourers'] = {
            42: {'foo': 'bar'},
        }
        orchestrator = Orchestrator(custom_config)

        self.assertEqual(orchestrator.get_labourer_setting(Labourer(id=42), 'foo'), 'bar')
        self.assertEqual(orchestrator.get_labourer_setting(Labourer(id=42), 'faz'), None)

        self.assertEqual(orchestrator.get_labourer_setting(Labourer(id=4422), 'foo'), None)
        self.assertEqual(orchestrator.get_labourer_setting(Labourer(id=4422), 'faz'), None)


    def test_get_desired_invocation_number_for_labourer(self):

        # Status - expected output for max invocations = 5
        TESTS = {
            0: 0,
            1: 0,
            2: 2,
            3: 3,
            4: 5
        }

        self.custom_config['labourers'] = {42: {'max_simultaneous_invocations': 5}}
        self.orchestrator = Orchestrator(self.custom_config)

        self.orchestrator.ecology_client = MagicMock()

        for eco, expected in TESTS.items():
            self.orchestrator.ecology_client.get_labourer_status.return_value = eco

            self.assertEqual(self.orchestrator.get_desired_invocation_number_for_labourer(Labourer(id=42)), expected)


    def test_get_desired_invocation_number_for_labourer__default(self):

        # Status - expected output for max invocations = 2
        TESTS = {
            0: 0,
            1: 0,
            2: 1,
            3: 1,
            4: 2
        }

        self.orchestrator.ecology_client = MagicMock()

        for eco, expected in TESTS.items():
            self.orchestrator.ecology_client.get_labourer_status.return_value = eco

            self.assertEqual(self.orchestrator.get_desired_invocation_number_for_labourer(Labourer(id=1)), expected)
