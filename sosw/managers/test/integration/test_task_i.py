import boto3
import logging
import random
import time
import unittest
import os

from collections import defaultdict
from unittest.mock import MagicMock, patch


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.managers.task import TaskManager
from sosw.test.variables import TEST_CONFIG
from sosw.components.dynamo_db import DynamoDbClient, clean_dynamo_table


class TaskManager_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_CONFIG


    @classmethod
    def setUpClass(cls):
        """
        Clean the classic autotest table.
        """
        cls.TEST_CONFIG['init_clients'] = ['DynamoDb']

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


    def setup_tasks(self, status='available'):
        """ Some fake adding some scheduled tasks for some workers. """

        if status == 'available':
            greenfield = time.time() - random.randint(0, 10000)
        elif status == 'invoked':
            greenfield = time.time() + random.randint(1, 100) + self.manager.config['greenfield_invocation_delta']
        else:
            raise ValueError(f"Unsupported `status`: {status}. Should be one of: 'available', 'invoked'.")

        for worker_id in range(42, 45):
            for i in range(3):
                row = {
                    'hash_col':      f"task_id_{worker_id}_{i}_{random.randint(0, 10000)}",  # Task ID
                    'range_col':     worker_id,  # Worker ID
                    'other_int_col': greenfield
                }
                self.dynamo_client.put(row, self.table_name)
                time.sleep(0.1)  # Sleep a little to fit the Write Capacity (10 WCU) of autotest table.


    def test_get_next_for_worker(self):
        self.setup_tasks()
        # time.sleep(5)

        result = self.manager.get_next_for_worker(worker_id=42)
        # print(result)

        self.assertEqual(len(result), 1, "Returned more than one task")
        self.assertEqual(result[0]['range_col'], 42, "Returned task of some other worker")
        self.assertIn('task_id_42_', result[0]['hash_col'])


    def test_get_next_for_worker__multiple(self):
        self.setup_tasks()

        result = self.manager.get_next_for_worker(worker_id=42, cnt=50)
        # print(result)

        self.assertEqual(len(result), 3, "Should be just 3 tasks for this worker in setup")
        self.assertTrue(all(task['range_col'] == 42 for task in result), "Returned some tasks of other Workers")


    def test_get_next_for_worker__not_take_invoked(self):
        self.setup_tasks()
        self.setup_tasks(status='invoked')

        result = self.manager.get_next_for_worker(worker_id=42, cnt=50)
        # print(result)

        self.assertEqual(len(result), 3, "Should be just 3 tasks for this worker in setup")
        self.assertTrue(all(task['range_col'] == 42 for task in result), "Returned some tasks of other Workers")
