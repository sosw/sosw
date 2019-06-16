import boto3
import os
import unittest

from copy import deepcopy
from unittest import mock
from unittest.mock import MagicMock, patch

from sosw.orchestrator import Orchestrator
from sosw.labourer import Labourer
from sosw.test.variables import TEST_ORCHESTRATOR_CONFIG


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Orchestrator_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_ORCHESTRATOR_CONFIG

    LABOURER = Labourer(id='some_function', arn='arn:aws:lambda:us-west-2:000000000000:function:some_function')

    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.custom_config = deepcopy(self.TEST_CONFIG)
        with patch('boto3.client'):
            self.orchestrator = Orchestrator(self.custom_config)



    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass


    def test_true(self):
        self.assertEqual(1, 1)


    # @unittest.skip("Depricated method")
    # def test_get_labourer_setting(self):
    #
    #     custom_config = self.TEST_CONFIG.copy()
    #     custom_config['labourers'] = {
    #         42: {'foo': 'bar'},
    #     }
    #
    #     with patch('boto3.client'):
    #         orchestrator = Orchestrator(custom_config)
    #
    #     self.assertEqual(orchestrator.get_labourer_setting(Labourer(id=42), 'foo'), 'bar')
    #     self.assertEqual(orchestrator.get_labourer_setting(Labourer(id=42), 'faz'), None)
    #
    #     self.assertEqual(orchestrator.get_labourer_setting(Labourer(id=4422), 'foo'), None)
    #     self.assertEqual(orchestrator.get_labourer_setting(Labourer(id=4422), 'faz'), None)


    def test_get_desired_invocation_number_for_labourer(self):

        # Status - expected output for max invocations = 10
        TESTS = {
            0: 0,
            1: 0,
            2: 5,
            3: 7,
            4: 10
        }

        some_labourer = self.orchestrator.task_client.register_labourers()[0]

        with patch('boto3.client'):
            orchestrator = Orchestrator(self.custom_config)

        orchestrator.task_client = MagicMock()

        print(some_labourer.get_attr('max_simultaneous_invocations'))
        for eco, expected in TESTS.items():
            orchestrator.task_client.ecology_client.get_labourer_status.return_value = eco
            orchestrator.task_client.ecology_client.count_running_tasks_for_labourer.return_value = 0
            self.assertEqual(orchestrator.get_desired_invocation_number_for_labourer(some_labourer), expected)


    def test_get_desired_invocation_number_for_labourer__default(self):

        # Status - expected output for max invocations = 2
        TESTS = {
            0: 0,
            1: 0,
            2: 5,
            3: 7,
            4: 10
        }

        # self.orchestrator.task_client.register_labourers.return_value = [self.LABOURER]
        # self.orchestrator.task_client.ecology_client.count_running_tasks_for_labourer.return_value = 0

        some_labourer = self.orchestrator.task_client.register_labourers()[0]

        # Once registered Labourers we Mock the task client.
        self.orchestrator.task_client = MagicMock()
        self.orchestrator.task_client.ecology_client.count_running_tasks_for_labourer.return_value = 0

        for eco, expected in TESTS.items():
            self.orchestrator.task_client.ecology_client.get_labourer_status.return_value = eco

            self.assertEqual(self.orchestrator.get_desired_invocation_number_for_labourer(some_labourer), expected)


    def test_invoke_for_labourer(self):
        TEST_COUNT = 3
        some_labourer = self.orchestrator.task_client.register_labourers()[0]

        self.orchestrator.get_desired_invocation_number_for_labourer = MagicMock(return_value=TEST_COUNT)

        self.orchestrator.invoke_for_labourer(some_labourer)

        self.orchestrator.get_desired_invocation_number_for_labourer.assert_called_once()


    def test_invoke_for_labourer__desired_zero(self):
        self.orchestrator.get_desired_invocation_number_for_labourer = MagicMock(return_value=0)
        self.orchestrator.task_client.invoke_task = MagicMock()

        self.orchestrator.invoke_for_labourer(self.LABOURER)

        self.orchestrator.task_client.invoke_task.assert_not_called()
