import logging
import random
import time
import unittest
import os

from unittest.mock import Mock, MagicMock, patch


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.managers.task import TaskManager
from sosw.labourer import Labourer
from sosw.test.variables import TEST_TASK_CLIENT_CONFIG, RETRY_TASKS, TASKS
from sosw.components.dynamo_db import DynamoDbClient, clean_dynamo_table
from sosw.components.helpers import first_or_none


class TaskManager_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_TASK_CLIENT_CONFIG
    LABOURER = Labourer(id='some_lambda', arn='arn:aws:lambda:some_lambda')


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

        self.HASH_KEY = ('task_id', 'S')
        self.RANGE_KEY = ('labourer_id', 'S')
        self.table_name = self.config['dynamo_db_config']['table_name']
        self.completed_tasks_table = self.config['sosw_closed_tasks_table']
        self.retry_tasks_table = self.config['sosw_retry_tasks_table']

        self.clean_task_tables()

        self.dynamo_client = DynamoDbClient(config=self.config['dynamo_db_config'])
        self.manager = TaskManager(custom_config=self.config)
        self.manager.ecology_client = MagicMock()

    def tearDown(self):
        self.clean_task_tables()


    def clean_task_tables(self):
        clean_dynamo_table(self.table_name, (self.HASH_KEY[0], self.RANGE_KEY[0]))
        clean_dynamo_table(self.completed_tasks_table, ('task_id',))
        clean_dynamo_table(self.retry_tasks_table, ('labourer_id', 'task_id'))


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


    # @unittest.skip("Function currently depricated")
    # def test_close_task(self):
    #     _ = self.manager.get_db_field_name
    #     # Create task with id=123
    #     task = {_('task_id'): '123', _('labourer_id'): 'lambda1', _('greenfield'): 8888, _('attempts'): 2,
    #             _('completed_at'): 123123}
    #     self.dynamo_client.put(task)
    #
    #     # Call
    #     self.manager.close_task(task_id='123', labourer_id='lambda1')
    #
    #     # Get from db, check
    #     tasks = self.dynamo_client.get_by_query({_('task_id'): '123'})
    #     self.assertEqual(len(tasks), 1)
    #     task_result = tasks[0]
    #
    #     expected_result = task.copy()
    #
    #     for k in ['task_id', 'labourer_id', 'greenfield', 'attempts']:
    #         assert expected_result[k] == task_result[k]
    #
    #     self.assertTrue(_('closed_at') in task_result, msg=f"{_('closed_at')} not in task_result {task_result}")
    #     self.assertTrue(time.time() - 360 < task_result[_('closed_at')] < time.time())


    def test_archive_task(self):
        _ = self.manager.get_db_field_name
        # Create task with id=123
        task = {_('task_id'): '123', _('labourer_id'): 'lambda1', _('greenfield'): 8888, _('attempts'): 2}
        self.dynamo_client.put(task)

        # Call
        self.manager.archive_task('123')

        # Check the task isn't in the tasks db, but is in the completed_tasks table
        tasks = self.dynamo_client.get_by_query({_('task_id'): '123', _('labourer_id'): 'lambda1'})
        self.assertEqual(len(tasks), 0)

        completed_tasks = self.dynamo_client.get_by_query({_('task_id'): '123'}, table_name=self.completed_tasks_table)
        self.assertEqual(len(completed_tasks), 1)
        completed_task = completed_tasks[0]

        for k in task.keys():
            self.assertEqual(task[k], completed_task[k])
        for k in completed_task.keys():
            if k != _('closed_at'):
                self.assertEqual(task[k], completed_task[k])

        self.assertTrue(time.time() - 360 < completed_task[_('closed_at')] < time.time())


    def test_move_task_to_retry_table(self):
        _ = self.manager.get_db_field_name
        labourer_id = 'lambda1'
        task = {_('task_id'): '123', _('labourer_id'): labourer_id, _('greenfield'): 8888, _('attempts'): 2}
        delay = 300

        self.dynamo_client.put(task)

        self.manager.move_task_to_retry_table(task, delay)

        result_tasks = self.dynamo_client.get_by_query({_('task_id'): '123', _('labourer_id'): labourer_id})
        self.assertEqual(len(result_tasks), 0)

        result_retry_tasks = self.dynamo_client.get_by_query({_('labourer_id'): labourer_id}, table_name=self.retry_tasks_table)
        self.assertEqual(len(result_retry_tasks), 1)
        result = first_or_none(result_retry_tasks)

        for k in task:
            self.assertEqual(task[k], result[k])
        for k in result:
            if k != _('desired_launch_time'):
                self.assertEqual(result[k], task[k])

        self.assertTrue(time.time() + delay - 60 < result[_('desired_launch_time')] < time.time() + delay + 60)


    def test_get_tasks_to_retry_for_labourer(self):
        _ = self.manager.get_db_field_name
        labourer_id = 'some_lambda'
        tasks = RETRY_TASKS.copy()
        # Add tasks to retry table
        for task in tasks:
            self.dynamo_client.put(task, self.config['sosw_retry_tasks_table'])

        # Call
        with patch('time.time') as t:
            t.return_value = 9500
            result_tasks = self.manager.get_tasks_to_retry_for_labourer(labourer_id, limit=20)

            self.assertEqual(len(result_tasks), 2)

            # Check it only gets tasks with timestamp <= now
            self.assertIn(tasks[0], result_tasks)
            self.assertIn(tasks[1], result_tasks)


    def test_retry_tasks(self):
        _ = self.manager.get_db_field_name
        labourer_id = 'some_lambda'

        self.manager.get_oldest_greenfield_for_labourer = Mock(return_value=8888)

        # Add tasks to tasks_table
        regular_tasks = [
            {_('labourer_id'): labourer_id, _('task_id'): '11', _('arn'): 'some_arn', _('payload'): {}, _('greenfield'): 8888},
            {_('labourer_id'): labourer_id, _('task_id'): '22', _('arn'): 'some_arn', _('payload'): {}, _('greenfield'): 9999},
        ]
        for task in regular_tasks:
            self.dynamo_client.put(task)

        # Add tasks to retry_table
        retry_tasks = RETRY_TASKS.copy()

        for task in retry_tasks:
            self.dynamo_client.put(task, table_name=self.config['sosw_retry_tasks_table'])

        # Use get_tasks_to_retry_for_labourer to get tasks
        tasks = self.manager.get_tasks_to_retry_for_labourer(labourer_id, limit=100)

        # Call
        self.manager.retry_tasks(labourer_id, tasks)

        # Check tasks moved to `tasks_table` with lowest greenfields
        retry_table_items = self.dynamo_client.get_by_scan(table_name=self.retry_tasks_table)
        self.assertEqual(len(retry_table_items), 0)

        tasks_table_items = self.dynamo_client.get_by_scan()
        self.assertEqual(len(tasks_table_items), 5)

        for reg_task in regular_tasks:
            self.assertIn(reg_task, tasks_table_items)

        for retry_task in retry_tasks:
            matching = [x for x in tasks_table_items if x[_('task_id')] == retry_task[_('task_id')]][0]

            for k in retry_task.keys():
                if k not in [_('greenfield'), _('desired_launch_time')]:
                    self.assertEqual(retry_task[k], matching[k])

            for k in matching.keys():
                if k != _('greenfield'):
                    self.assertEqual(retry_task[k], matching[k])

            self.assertTrue(8880 < matching[_('greenfield')] < 8888)


    def test_get_oldest_greenfield_for_labourer(self):
        min_gf = 20000
        for i in range(5):  # Ran this with range(1000), it passes :)
            gf = random.randint(10000, 20000)
            if gf < min_gf:
                min_gf = gf
            row = {'labourer_id': f"some_lambda", 'task_id': f"task-{i}", 'greenfield': gf}
            self.dynamo_client.put(row)
            time.sleep(0.1)  # Sleep a little to fit the Write Capacity (10 WCU) of autotest table.

        result = self.manager.get_oldest_greenfield_for_labourer('some_lambda')

        self.assertEqual(min_gf, result)


    def test_get_length_of_queue_for_labourer(self):
        labourer = Labourer(id='some_lambda', arn='some_arn')

        num_of_tasks = 3  # Ran this with 464 tasks and it worked

        for i in range(num_of_tasks):
            row = {'labourer_id': f"some_lambda", 'task_id': f"task-{i}", 'greenfield': i}
            self.dynamo_client.put(row)
            time.sleep(0.1)  # Sleep a little to fit the Write Capacity (10 WCU) of autotest table.

        queue_len = self.manager.get_length_of_queue_for_labourer(labourer)

        self.assertEqual(queue_len, num_of_tasks)
