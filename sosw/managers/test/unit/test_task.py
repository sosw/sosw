import boto3
import logging
import os
import random
import time
import unittest

from collections import defaultdict
from unittest.mock import MagicMock, patch


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.managers.task import TaskManager
from sosw.labourer import Labourer
from sosw.test.variables import TEST_CONFIG


class task_manager_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_CONFIG['task_client_config']

    LABOURER = Labourer(id=1, arn='bar')


    def setUp(self):
        """
        We keep copies of main parameters here, because they may differ from test to test and cleanup needs them.
        This is responsibility of the test author to update these values if required from test.
        """

        self.HASH_KEY = ('hash_col', 'S')
        self.RANGE_KEY = ('range_col', 'N')
        self.KEYS = ('hash_col', 'range_col')
        self.table_name = 'autotest_dynamo_db'

        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.config = self.TEST_CONFIG.copy()
        self.manager = TaskManager(custom_config=self.config)
        self.manager.dynamo_db_client = MagicMock()
        self.manager.lambda_client = MagicMock()


    def tearDown(self):
        self.patcher.stop()


    def test_get_max_univoked_greenfield(self):
        """ We allow round -2 here. The values are big, and we want to minimize random failures. """
        self.assertAlmostEqual(round(time.time() + self.manager.config['greenfield_invocation_delta'], -2),
                               round(self.manager._get_max_univoked_greenfield(), -2))


    def test_get_db_field_name(self):
        self.assertEqual(self.manager.get_db_field_name('task_id'), 'hash_col', "Configured field name failed")
        self.assertEqual(self.manager.get_db_field_name('some_name'), 'some_name', "Default column name failed")


    def test_mark_task_invoked__calls_dynamo(self):
        self.manager.dynamo_db_client = MagicMock()

        greenfield = round(time.time() - random.randint(0, 1000))
        delta = self.manager.config['greenfield_invocation_delta']

        task = {
            'hash_col':      "task_id_42_256",  # Task ID
            'range_col':     42,  # Worker ID
            'other_int_col': greenfield
        }

        # Do the actual tested job
        self.manager.mark_task_invoked(task)

        # Check the dynamo_client was called with correct payload to update
        self.manager.dynamo_db_client.update.assert_called_once()

        call_args, call_kwargs = self.manager.dynamo_db_client.update.call_args

        self.assertEqual(call_args[0], {'hash_col': "task_id_42_256", 'range_col': 42}), "The key of task is missing"
        self.assertEqual(call_kwargs['attributes_to_increment'], {'attempts': 1}), "Attempts counter not increased"

        gf = call_kwargs['attributes_to_update']['other_int_col']
        self.assertEqual(round(gf, -2), round(time.time() + delta, -2)), "Greenfield was not updated"


    def test_mark_task_invoked__greenfield_counts_attempts(self):
        self.manager.dynamo_db_client = MagicMock()

        greenfield = round(time.time() - random.randint(0, 1000))
        delta = self.manager.config['greenfield_invocation_delta']

        task = {
            'hash_col':      "task_id_42_256",  # Task ID
            'range_col':     42,  # Worker ID
            'other_int_col': greenfield,
            'attempts':      3
        }

        # Do the actual tested job
        self.manager.mark_task_invoked(task)

        # Check the dynamo_client was called with correct payload to update
        self.manager.dynamo_db_client.update.assert_called_once()

        call_args, call_kwargs = self.manager.dynamo_db_client.update.call_args

        self.assertEqual(call_args[0], {'hash_col': "task_id_42_256", 'range_col': 42}), "The key of task is missing"
        self.assertEqual(call_kwargs['attributes_to_increment'], {'attempts': 1}), "Attempts counter not increased"

        gf = call_kwargs['attributes_to_update']['other_int_col']
        self.assertEqual(round(gf, -2), round(time.time() + delta * 4, -2),
                         "Greenfield was increased with respect to number of attempts")


    def test_invoke_task__validates_task(self):
        self.assertRaises(AttributeError, self.manager.invoke_task, labourer=self.LABOURER), "Missing task and task_id"
        self.assertRaises(AttributeError, self.manager.invoke_task, labourer=self.LABOURER, task_id='qwe',
                          task={1: 2}), "Both task and task_id."


    def test_invoke_task__calls__mark_task_invoked(self):
        self.manager.mark_task_invoked = MagicMock()

        self.manager.invoke_task(task_id='qwe', labourer=self.LABOURER)
        self.manager.mark_task_invoked.assert_called_once()


    def test_invoke_task__calls__get_task_by_id(self):
        self.manager.get_task_by_id = MagicMock()

        self.manager.invoke_task(task_id='qwe', labourer=self.LABOURER)
        self.manager.get_task_by_id.assert_called_once()


    def test_invoke_task__calls__lambda_client(self):
        task = {
            'hash_col':  "task_id_42_256",  # Task ID
            'range_col': 42,  # Worker ID
            'payload':   {'foo': 23}
        }

        self.manager.get_task_by_id = MagicMock(return_value=task)

        self.manager.invoke_task(task_id='task_id_42_256', labourer=self.LABOURER)

        self.manager.lambda_client.invoke.assert_called_once()

        call_args, call_kwargs = self.manager.lambda_client.invoke.call_args

        self.assertEqual(call_kwargs['FunctionName'], self.LABOURER.arn)
        self.assertEqual(call_kwargs['Payload'], task['payload'])
