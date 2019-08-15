import boto3
import os
import random
import unittest

from unittest.mock import MagicMock, patch

from sosw.orchestrator import Orchestrator
from sosw.labourer import Labourer
from sosw.test.variables import TEST_ORCHESTRATOR_CONFIG, TEST_TASK_CLIENT_CONFIG
from sosw.test.helpers_test import line_count


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Scheduler_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_ORCHESTRATOR_CONFIG
    LABOURER = Labourer(id='some_function', arn='arn:aws:lambda:us-west-2:000000000000:function:some_function')


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.custom_config = self.TEST_CONFIG.copy()
        self.orchestrator = Orchestrator(self.custom_config)
        self.orchestrator.task_client.ecology_client = MagicMock()
        self.orchestrator.task_client.ecology_client.get_labourer_status.return_value = 4
        self.orchestrator.task_client.ecology_client.count_running_tasks_for_labourer.return_value = 0


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass


    def test_call(self):
        self.orchestrator({'event': 42})

        some_labourer = self.orchestrator.task_client.register_labourers()[0]
        # self.orchestrator.task_client.ecology_client.count_running_tasks_for_labourer(some_labourer)
