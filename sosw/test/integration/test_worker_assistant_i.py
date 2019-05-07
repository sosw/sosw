import datetime

import logging
import unittest
import os
from unittest.mock import patch

from sosw.worker_assistant import WorkerAssistant


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.test.variables import TEST_TASK_CLIENT_CONFIG
from sosw.components.dynamo_db import DynamoDbClient, clean_dynamo_table


class WorkerAssistant_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_TASK_CLIENT_CONFIG


    @classmethod
    def setUpClass(cls):
        """
        Clean the classic autotest table.
        """
        cls.TEST_CONFIG['init_clients'] = ['DynamoDb']


    def setUp(self):
        """
        We keep copies of main parameters here, because they may differ from test to test and cleanup needs them.
        This is responsibility of the test author to update these values if required from test.
        """
        self.config = self.TEST_CONFIG.copy()

        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.table_name = self.config['dynamo_db_config']['table_name']
        self.HASH_KEY = ('task_id', 'S')

        self.clean_task_tables()

        self.dynamo_client = DynamoDbClient(config=self.config['dynamo_db_config'])

        self.assistant = WorkerAssistant(custom_config={'test': 1})


    def tearDown(self):
        self.patcher.stop()
        self.clean_task_tables()


    def clean_task_tables(self):
        clean_dynamo_table(self.table_name, (self.HASH_KEY[0],))


    def test_mark_task_as_completed(self):
        _ = self.assistant.get_db_field_name
        task_id = '123'

        initial_task = {_('task_id'): task_id, _('labourer_id'): 'lab', _('greenfield'): 8888, _('attempts'): 2}
        self.dynamo_client.put(initial_task)

        between_times = (
            (datetime.datetime.now() - datetime.timedelta(minutes=1)).timestamp(),
            (datetime.datetime.now() + datetime.timedelta(minutes=1)).timestamp()
        )

        self.assistant.mark_task_as_completed(task_id)

        changed_task = self.dynamo_client.get_by_query({_('task_id'): task_id})[0]

        self.assertTrue(between_times[0] <= changed_task['completed_at'] <= between_times[1],
                        msg=f"NOT {between_times[0]} <= {changed_task['completed_at']} <= {between_times[1]}")
