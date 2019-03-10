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
from sosw.labourer import Labourer
from sosw.test.variables import TEST_TASK_CLIENT_CONFIG
from sosw.components.dynamo_db import DynamoDbClient, clean_dynamo_table


class TaskManager_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_TASK_CLIENT_CONFIG
    LABOURER = Labourer(id='some_lambda', arn='arn:aws:lambda:some_lambda')


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
        self.config = self.TEST_CONFIG.copy()

        self.HASH_KEY = ('task_id', 'S')
        self.RANGE_KEY = ('labourer_id', 'S')
        self.table_name = self.config['dynamo_db_config']['table_name']
        self.completed_tasks_table = self.config['sosw_closed_tasks_table']

        self.dynamo_client = DynamoDbClient(config=self.config['dynamo_db_config'])
        self.manager = TaskManager(custom_config=self.config)
        self.manager.ecology_client = MagicMock()

    def tearDown(self):
        clean_dynamo_table(self.table_name, (self.HASH_KEY[0], self.RANGE_KEY[0]))
        clean_dynamo_table(self.completed_tasks_table, ('task_id',))


    def setup_tasks(self, status='available', mass=False):
        """ Some fake adding some scheduled tasks for some workers. """

        if status == 'available':
            greenfield = round(time.time()) - random.randint(0, 10000)
        elif status == 'invoked':
            greenfield = round(time.time()) + self.manager.config['greenfield_invocation_delta']
        elif status == 'expired':
            greenfield = round(time.time()) + random.randint(1000, 10000)
        elif status == 'running':
            greenfield = round(time.time()) + self.manager.config['greenfield_invocation_delta'] - 900
        else:
            raise ValueError(f"Unsupported `status`: {status}. Should be one of: 'available', 'invoked'.")

        workers = [self.LABOURER.id] if not mass else range(42, 45)
        for worker_id in workers:
            for i in range(3):
                row = {
                    self.HASH_KEY[0]:  f"task_id_{worker_id}_{i}_{random.randint(0, 10000)}",  # Task ID
                    self.RANGE_KEY[0]: str(worker_id),  # Worker ID
                    'greenfield':      greenfield
                }
                print(f"Putting {row} to {self.table_name}")
                self.dynamo_client.put(row, self.table_name)
                time.sleep(0.1)  # Sleep a little to fit the Write Capacity (10 WCU) of autotest table.


    def test_get_next_for_worker(self):
        self.setup_tasks()
        # time.sleep(5)

        result = self.manager.get_next_for_labourer(self.LABOURER)
        # print(result)

        self.assertEqual(len(result), 1, "Returned more than one task")
        self.assertIn(f'task_id_{self.LABOURER.id}_', result[0])


    def test_get_next_for_worker__multiple(self):
        self.setup_tasks()

        result = self.manager.get_next_for_labourer(self.LABOURER, cnt=5000)
        # print(result)

        self.assertEqual(len(result), 3, "Should be just 3 tasks for this worker in setup")
        self.assertTrue(all(f'task_id_{self.LABOURER.id}_' in task for task in result),
                        "Returned some tasks of other Workers")


    def test_get_next_for_worker__not_take_invoked(self):
        self.setup_tasks()
        self.setup_tasks(status='invoked')

        result = self.manager.get_next_for_labourer(self.LABOURER, cnt=50)
        # print(result)

        self.assertEqual(len(result), 3, "Should be just 3 tasks for this worker in setup. The other 3 are invoked.")
        self.assertTrue(all(f'task_id_{self.LABOURER.id}_' in task for task in result),
                        "Returned some tasks of other Workers")


    def register_labourers(self):
        self.manager.get_labourers = MagicMock(return_value=[self.LABOURER])
        self.manager.register_labourers()


    def test_mark_task_invoked(self):
        greenfield = round(time.time() - random.randint(100, 1000))
        delta = self.manager.config['greenfield_invocation_delta']
        self.register_labourers()

        row = {
            self.HASH_KEY[0]:  f"task_id_{self.LABOURER.id}_256",  # Task ID
            self.RANGE_KEY[0]: self.LABOURER.id,  # Worker ID
            'greenfield':      greenfield
        }
        self.dynamo_client.put(row)
        # print(f"Saved initial version with greenfield some date not long ago: {row}")

        # Do the actual tested job
        self.manager.mark_task_invoked(self.LABOURER, row)
        time.sleep(1)
        result = self.dynamo_client.get_by_query({self.HASH_KEY[0]: f"task_id_{self.LABOURER.id}_256"}, strict=False)
        # print(f"The new updated value of task is: {result}")

        # Rounded -2 we check that the greenfield was updated
        self.assertAlmostEqual(round(int(time.time()) + delta, -2), round(result[0]['greenfield'], -2))


    def test_get_invoked_tasks_for_labourer(self):
        self.register_labourers()

        self.setup_tasks(status='running')
        self.setup_tasks(status='expired')
        self.setup_tasks(status='invoked')
        self.assertEqual(len(self.manager.get_invoked_tasks_for_labourer(self.LABOURER)), 3)


    def test_get_running_tasks_for_labourer(self):
        self.register_labourers()

        self.setup_tasks(status='available')
        self.setup_tasks(status='running')
        self.setup_tasks(status='expired')
        self.assertEqual(len(self.manager.get_running_tasks_for_labourer(self.LABOURER)), 3)


    def test_get_expired_tasks_for_labourer(self):
        self.register_labourers()

        self.setup_tasks(status='running')
        self.setup_tasks(status='expired')
        self.assertEqual(len(self.manager.get_expired_tasks_for_labourer(self.LABOURER)), 3)


    def test_close_task(self):
        _ = self.manager.get_db_field_name
        # Create task with id=123
        task = {_('task_id'): '123', _('labourer_id'): 'lambda1', _('greenfield'): 8888, _('attempts'): 2}
        self.dynamo_client.put(task)

        # Call
        self.manager.close_task(task_id='123', labourer_id='lambda1', completed=True)

        # Get from db, check
        tasks = self.dynamo_client.get_by_query({_('task_id'): '123'})
        self.assertEqual(len(tasks), 1)
        task_result = tasks[0]

        expected_result = task.copy()
        expected_result['completed'] = 1

        for k in ['task_id', 'labourer_id', 'greenfield', 'attempts', 'completed']:
            assert expected_result[k] == task_result[k]

        self.assertTrue(_('closed_at') in task_result, msg=f"{_('closed_at')} not in task_result {task_result}")
        self.assertTrue(time.time() - 360 < task_result[_('closed_at')] < time.time())


    def test_archive_task(self):
        _ = self.manager.get_db_field_name
        # Create task with id=123
        task = {_('task_id'): '123', _('labourer_id'): 'lambda1', _('greenfield'): 8888, _('attempts'): 2,
                _('closed_at'): 22332233, _('completed'): 1}
        self.dynamo_client.put(task)

        # Call
        self.manager.archive_task('123')

        # Check the task isn't in the tasks db, but is in the completed_tasks table
        tasks = self.dynamo_client.get_by_query({_('task_id'): '123', _('labourer_id'): 'lambda1'})
        self.assertEqual(len(tasks), 0)

        completed_tasks = self.dynamo_client.get_by_query({_('task_id'): '123'}, table_name=self.completed_tasks_table)
        self.assertEqual(len(completed_tasks), 1)
        self.assertEqual(completed_tasks[0], task)
