import boto3
import logging
import time
import unittest
import os

from collections import defaultdict
from unittest.mock import MagicMock, patch


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.managers.task import TaskManager
from sosw.components.dynamo_db import DynamoDbClient, clean_dynamo_table


class TaskManager_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = {
        # 'init_clients':            [],
        'dynamo_db_config': {
            'row_mapper':       {
                'hash_col':  'S',
                'range_col': 'N',
            },
            'required_fields':  ['hash_col'],
            'table_name':       'autotest_dynamo_db',
            'index_greenfield': 'autotest_index',
        },
        'greenfield_field_name': "" #TODO CONTINUE HERE!
    }


    @classmethod
    def setUpClass(cls):
        """
        Clean the classic autotest table.
        """
        clean_dynamo_table()


    def setUp(self):
        """
        We keep copies of main parameters here, because they may differ from test to test and cleanup needs them.
        This is responsibility of the test author to update these values if required from test.
        """
        self.HASH_KEY = ('hash_col', 'S')
        self.RANGE_KEY = ('range_col', 'N')
        self.KEYS = ('hash_col', 'range_col')
        self.table_name = 'autotest_dynamo_db'

        self.config = self.TEST_CONFIG.copy()
        self.dynamo_client = DynamoDbClient(config=self.config['dynamo_db_config'])
        self.manager = TaskManager(custom_config=self.config)


    def tearDown(self):
        clean_dynamo_table(self.table_name, self.KEYS)


    # def test_true(self):
    #     self.assertEqual(1, 2)

    def setup_tasks(self):
        for hk in range(42, 45):
            for i in range(10):
                row = {'hash_col': f"{hk}", 'range_col': i}
                self.dynamo_client.put(row, self.table_name)
                time.sleep(0.1)  # Sleep a little to fit the Write Capacity (10 WCU) of autotest table.


    def test_get_next_for_worker(self):
        self.setup_tasks()

        result = self.manager.get_next_for_worker(worker_id=42)
        print(result)
        self.assertEqual(result, 42)
