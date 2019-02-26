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
from sosw.test.variables import TEST_CONFIG


class TaskManager_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_CONFIG['task_client_config']


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
        self.manager = TaskManager(custom_config=self.config)


    def tearDown(self):
        pass


    def test_get_max_univoked_greenfield(self):
        """ We allow round -2 here. The values are big, and we want to minimize random failures. """
        self.assertAlmostEqual(round(time.time() + self.manager.config['greenfield_invocation_delta'], -2),
                               round(self.manager._get_max_univoked_greenfield(), -2))


    def test_get_db_field_name(self):
        self.assertEqual(self.manager.get_db_field_name('task_id'), 'hash_col', "Configured field name failed")
        self.assertEqual(self.manager.get_db_field_name('some_name'), 'some_name', "Default column name failed")
