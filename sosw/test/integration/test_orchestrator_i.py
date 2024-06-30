import asyncio
import os
import unittest

from unittest.mock import MagicMock, patch

from sosw.orchestrator import Orchestrator
from sosw.labourer import Labourer
from sosw.test.variables import TEST_ORCHESTRATOR_CONFIG
from sosw.test.helpers_test_dynamo_db import AutotestDdbManager, autotest_dynamo_db_tasks_setup

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Orchestrator_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_ORCHESTRATOR_CONFIG
    LABOURER = Labourer(id='some_function', arn='arn:aws:lambda:us-west-2:000000000000:function:some_function')

    autotest_ddbm: AutotestDdbManager = None

    @classmethod
    def setUpClass(cls) -> None:
        tables = [autotest_dynamo_db_tasks_setup]
        cls.autotest_ddbm = AutotestDdbManager(tables)


    @classmethod
    def tearDownClass(cls) -> None:
        asyncio.run(cls.autotest_ddbm.drop_ddbs())


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()
        self.get_config_patch.return_value = {}

        self.custom_config = self.TEST_CONFIG.copy()
        self.custom_config['init_clients'] = ['S3', ]

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
