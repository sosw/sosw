import boto3
import logging
import os
import random
import time
import unittest

from collections import defaultdict
from copy import deepcopy
from unittest.mock import Mock, MagicMock, patch


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.labourer import Labourer
from sosw.managers.task import TaskManager
from sosw.test.variables import TEST_TASK_CLIENT_CONFIG


class task_manager_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_TASK_CLIENT_CONFIG

    LABOURER = Labourer(id='some_function', arn='arn:aws:lambda:us-west-2:0000000000:function:some_function')


    def setUp(self):
        """
        We keep copies of main parameters here, because they may differ from test to test and cleanup needs them.
        This is responsibility of the test author to update these values if required from test.
        """

        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.config = self.TEST_CONFIG.copy()

        self.labourer = deepcopy(self.LABOURER)

        self.HASH_KEY = ('task_id', 'S')
        self.RANGE_KEY = ('labourer_id', 'S')
        self.table_name = self.config['dynamo_db_config']['table_name']

        self.manager = TaskManager(custom_config=self.config)
        self.manager.dynamo_db_client = MagicMock()
        self.manager.ecology_client = MagicMock()
        self.manager.ecology_client.get_labourer_status.return_value = 2
        self.manager.lambda_client = MagicMock()


    def tearDown(self):
        self.patcher.stop()


    def test_get_db_field_name(self):
        self.assertEqual(self.manager.get_db_field_name('task_id'), self.HASH_KEY[0], "Configured field name failed")
        self.assertEqual(self.manager.get_db_field_name('some_name'), 'some_name', "Default column name failed")


    def test_mark_task_invoked__calls_dynamo(self):
        self.manager.dynamo_db_client = MagicMock()
        self.manager.get_labourers = MagicMock(return_value=[self.labourer])
        self.manager.register_labourers()

        greenfield = round(time.time() - random.randint(0, 1000))
        delta = self.manager.config['greenfield_invocation_delta']

        task = {
            self.HASH_KEY[0]:  f"task_id_{self.labourer.id}_256",  # Task ID
            self.RANGE_KEY[0]: self.labourer.id,  # Worker ID
            'greenfield':      greenfield
        }

        # Do the actual tested job
        self.manager.mark_task_invoked(self.labourer, task)

        # Check the dynamo_client was called with correct payload to update
        self.manager.dynamo_db_client.update.assert_called_once()

        call_args, call_kwargs = self.manager.dynamo_db_client.update.call_args

        self.assertEqual(call_args[0],
                         {
                             self.HASH_KEY[0]: f"task_id_{self.labourer.id}_256", self.RANGE_KEY[0]: self.labourer.id
                         }), "The key of task is missing"
        self.assertEqual(call_kwargs['attributes_to_increment'], {'attempts': 1}), "Attempts counter not increased"

        gf = call_kwargs['attributes_to_update']['greenfield']
        self.assertEqual(round(gf, -2), round(time.time() + delta, -2)), "Greenfield was not updated"


    # @unittest.skip("This behavior is deprecated")
    # def test_mark_task_invoked__greenfield_counts_attempts(self):
    #     self.manager.dynamo_db_client = MagicMock()
    #
    #     greenfield = round(time.time() - random.randint(0, 1000))
    #     delta = self.manager.config['greenfield_invocation_delta']
    #
    #     task = {
    #         self.HASH_KEY[0]:  "task_id_42_256",  # Task ID
    #         self.RANGE_KEY[0]: 42,  # Worker ID
    #         'greenfield':      greenfield,
    #         'attempts':        3
    #     }
    #
    #     # Do the actual tested job
    #     self.manager.mark_task_invoked(task)
    #
    #     # Check the dynamo_client was called with correct payload to update
    #     self.manager.dynamo_db_client.update.assert_called_once()
    #
    #     call_args, call_kwargs = self.manager.dynamo_db_client.update.call_args
    #
    #     self.assertEqual(call_args[0],
    #                      {self.HASH_KEY[0]: "task_id_42_256", self.RANGE_KEY[0]: 42}), "The key of task is missing"
    #     self.assertEqual(call_kwargs['attributes_to_increment'], {'attempts': 1}), "Attempts counter not increased"
    #
    #     gf = call_kwargs['attributes_to_update']['greenfield']
    #     self.assertEqual(round(gf, -2), round(time.time() + delta * 4, -2),
    #                      "Greenfield was increased with respect to number of attempts")

    def test_invoke_task__validates_task(self):
        self.assertRaises(AttributeError, self.manager.invoke_task, labourer=self.labourer), "Missing task and task_id"
        self.assertRaises(AttributeError, self.manager.invoke_task, labourer=self.labourer, task_id='qwe',
                          task={1: 2}), "Both task and task_id."


    def test_invoke_task__calls__mark_task_invoked(self):
        self.manager.mark_task_invoked = MagicMock()

        self.manager.invoke_task(task_id='qwe', labourer=self.labourer)
        self.manager.mark_task_invoked.assert_called_once()


    def test_invoke_task__calls__get_task_by_id(self):
        self.manager.mark_task_invoked = MagicMock()
        self.manager.get_task_by_id = MagicMock()

        self.manager.invoke_task(task_id='qwe', labourer=self.labourer)
        self.manager.get_task_by_id.assert_called_once()


    def test_invoke_task__calls__lambda_client(self):
        self.manager.get_labourers = MagicMock(return_value=[self.labourer])
        self.manager.register_labourers()

        task = {
            self.HASH_KEY[0]:  f"task_id_{self.labourer.id}_256",  # Task ID
            self.RANGE_KEY[0]: self.labourer.id,  # Worker ID
            'payload':         {'foo': 23}
        }

        self.manager.get_task_by_id = MagicMock(return_value=task)

        self.manager.invoke_task(task_id=f'task_id_{self.labourer}_256', labourer=self.labourer)

        self.manager.lambda_client.invoke.assert_called_once()

        call_args, call_kwargs = self.manager.lambda_client.invoke.call_args

        self.assertEqual(call_kwargs['FunctionName'], self.labourer.arn)
        self.assertEqual(call_kwargs['Payload'], task['payload'])


    def test_invoke_task__not_calls__lambda_client_if_raised_conditional_exception(self):
        self.manager.register_labourers()

        task = {
            self.HASH_KEY[0]:  f"task_id_{self.labourer.id}_256",  # Task ID
            self.RANGE_KEY[0]: self.labourer.id,  # Worker ID
            'payload':         {'foo': 23}
        }


        class ConditionalCheckFailedException(Exception):
            pass


        self.manager.get_task_by_id = MagicMock(return_value=task)
        self.manager.mark_task_invoked = MagicMock()
        self.manager.mark_task_invoked.side_effect = ConditionalCheckFailedException("Boom")

        self.manager.invoke_task(task_id=f'task_id_{self.labourer}_256', labourer=self.labourer)

        self.manager.lambda_client.invoke.assert_not_called()
        self.assertEqual(self.manager.stats['concurrent_task_invocations_skipped'], 1)


    def test_register_labourers(self):
        with patch('time.time') as t:
            t.return_value = 123

            labourers = self.manager.register_labourers()

        lab = labourers[0]
        invoke_time = 123 + self.manager.config['greenfield_invocation_delta']

        self.assertEqual(lab.get_attr('start'), 123)
        self.assertEqual(lab.get_attr('invoked'), invoke_time)
        self.assertEqual(lab.get_attr('expired'), invoke_time - lab.duration - lab.cooldown)
        self.assertEqual(lab.get_attr('health'), 2)
        self.assertEqual(lab.get_attr('max_attempts'), 3)


    def test_calculate_count_of_running_tasks_for_labourer(self):
        lab = Labourer(id=42)
        self.manager.get_running_tasks_for_labourer = MagicMock(return_value=[1, 2, 3])

        self.assertEqual(self.manager.calculate_count_of_running_tasks_for_labourer(labourer=lab), 3)
        self.manager.get_running_tasks_for_labourer.assert_called_once()


    def test_get_labourers(self):
        self.config['labourers'] = {
            'some_lambda': {'foo': 'bar', 'arn': '123'},
            'some_lambda2': {'foo': 'baz'},
        }
        self.task_client = TaskManager(custom_config=self.config)

        result = self.task_client.get_labourers()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].foo, 'bar')
        self.assertEqual(result[0].arn, '123')
        self.assertEqual(result[1].foo, 'baz')


    def test_archive_task(self):
        task_id = '918273'
        task = {'labourer_id': 'some_lambda', 'task_id': task_id, 'payload': '{}', 'completed_at': '1551962375',
                'closed_at': '111'}

        # Mock
        self.manager.dynamo_db_client = MagicMock()
        self.manager.get_task_by_id = Mock(return_value=task)

        # Call
        self.manager.archive_task(task_id)

        # Check calls
        expected_completed_task = task.copy()
        expected_completed_task['labourer_id_task_status'] = 'some_lambda_1'
        self.manager.dynamo_db_client.put.assert_called_once_with(expected_completed_task, table_name=self.TEST_CONFIG['sosw_closed_tasks_table'])
        self.manager.dynamo_db_client.delete.assert_called_once_with({'labourer_id': 'some_lambda', 'task_id': task_id})


    # @unittest.skip("Function currently depricated")
    # def test_close_task(self):
    #     _ = self.manager.get_db_field_name
    #     task_id = '918273'
    #     labourer_id = 'some_lambda'
    #
    #     # Mock
    #     self.manager.dynamo_db_client = MagicMock()
    #
    #     # Call
    #     self.manager.close_task(task_id, 'some_lambda')
    #
    #     # Check calls
    #     self.manager.dynamo_db_client.update.assert_called_once_with(
    #             {_('task_id'): task_id, _('labourer_id'): labourer_id},
    #             attributes_to_update={_('closed_at'): int(time.time())})


    def move_task_to_retry_table(self):
        task_id = '123'
        task = {'labourer_id': 'some_lambda', 'task_id': task_id, 'payload': '{}'}
        delay = 350

        # Mock
        self.manager.dynamo_db_client = MagicMock()

        self.manager.move_task_to_retry_table(task, delay)

        retry_task = {'labourer_id': 'some_lambda', 'task_id': task_id, 'payload': '{}'}
        called_with_row = self.manager.dynamo_db_client.put.call_args[0][0]
        called_with_table = self.manager.dynamo_db_client.put.call_args[0][2]

        for k in retry_task:
            self.assertEqual(retry_task[k], called_with_row[k])
        for k in called_with_row:
            if k != 'desired_launch_time':
                self.assertEqual(retry_task[k], called_with_row[k])

        self.assertTrue(time.time() - 60 < called_with_row['desired_launch_time'] < time.time() + 60)

        self.assertEqual(called_with_table, self.config['sosw_retry_tasks_table'])
