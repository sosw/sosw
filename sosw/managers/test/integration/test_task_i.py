import asyncio
import boto3
import logging
import os
import random
import time
import unittest
import uuid

from copy import deepcopy
from unittest.mock import Mock, MagicMock, patch

logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.managers.task import TaskManager
from sosw.labourer import Labourer
from sosw.test.variables import TEST_TASK_CLIENT_CONFIG, RETRY_TASKS, TASKS
from sosw.components.dynamo_db import DynamoDbClient, clean_dynamo_table
from sosw.components.helpers import first_or_none
from sosw.test.helpers_test_dynamo_db import AutotestDdbManager, autotest_dynamo_db_tasks_setup, \
    autotest_dynamo_db_closed_tasks_setup, autotest_dynamo_db_retry_tasks_setup, safe_put_to_ddb


class TaskManager_IntegrationTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_TASK_CLIENT_CONFIG
    LABOURER = Labourer(id='some_function', arn='arn:aws:lambda:us-west-2:000000000000:function:some_function')

    autotest_ddbm: AutotestDdbManager = None


    @classmethod
    def setUpClass(cls):
        """
        Clean the classic autotest table.
        """
        cls.TEST_CONFIG['init_clients'] = ['DynamoDb']

        tables = [autotest_dynamo_db_tasks_setup, autotest_dynamo_db_closed_tasks_setup,
                  autotest_dynamo_db_retry_tasks_setup]
        cls.autotest_ddbm = AutotestDdbManager(tables)


    def setUp(self):
        """
        We keep copies of main parameters here, because they may differ from test to test and cleanup needs them.
        This is responsibility of the test author to update these values if required from test.
        """
        self.config = self.TEST_CONFIG.copy()

        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.HASH_KEY = ('task_id', 'S')
        self.RANGE_KEY = ('labourer_id', 'S')
        self.NOW_TIME = 100000

        self.table_name = self.config['dynamo_db_config']['table_name']
        self.completed_tasks_table = self.config['sosw_closed_tasks_table']
        self.retry_tasks_table = self.config['sosw_retry_tasks_table']

        self.dynamo_client = DynamoDbClient(config=self.config['dynamo_db_config'])
        self.manager = TaskManager(custom_config=self.config)
        self.manager.ecology_client = MagicMock()

        self.labourer = deepcopy(self.LABOURER)


    def tearDown(self):
        self.patcher.stop()
        asyncio.run(self.autotest_ddbm.clean_ddbs())


    @classmethod
    def tearDownClass(cls) -> None:
        asyncio.run(cls.autotest_ddbm.drop_ddbs())


    def setup_tasks(self, status='available', mutiple_labourers=False, count_tasks=3):
        """ Some fake adding some scheduled tasks for some workers. """

        _ = self.manager.get_db_field_name
        _cfg = self.manager.config.get

        table = _cfg('dynamo_db_config')['table_name'] if status not in ['closed', 'failed'] \
            else _cfg('sosw_closed_tasks_table')

        MAP = {
            'available': {
                self.RANGE_KEY[0]:               lambda x: str(worker_id),
                _('greenfield'):                 lambda x: round(10000 + random.randrange(0, 100000, 1000)),
                _('attempts'):                   lambda x: 0,
                _('result_uploaded_files'):      lambda x: [{'bucket':      'cnvm',
                                                             's3_key':      'key',
                                                             'description': 'description'
                                                             }],
                _('stat_time_register_clients'): lambda x: 0.00440446899847
            },
            'invoked':   {
                self.RANGE_KEY[0]: lambda x: str(worker_id),
                _('greenfield'):   lambda x: round(time.time()) + _cfg('greenfield_invocation_delta'),
                _('attempts'):     lambda x: 1,
            },
            'expired':   {
                self.RANGE_KEY[0]: lambda x: str(worker_id),
                _('greenfield'):   lambda x: round(time.time()) + _cfg('greenfield_invocation_delta')
                                             - random.randint(1000, 10000),
                _('attempts'):     lambda x: 1,
            },
            'running':   {
                self.RANGE_KEY[0]: lambda x: str(worker_id),
                _('greenfield'):   lambda x: round(time.time()) + _cfg('greenfield_invocation_delta')
                                             - random.randint(1, 900),
                _('attempts'):     lambda x: 1,
            },

            'closed':    {
                _('greenfield'):              lambda x: round(time.time()) + _cfg('greenfield_invocation_delta')
                                                        - random.randint(1000, 10000),
                _('labourer_id_task_status'): lambda x: f"{self.LABOURER.id}_1",

                _('completed_at'):            lambda x: x[_('greenfield')] - _cfg('greenfield_invocation_delta')
                                                        + random.randint(10, 300),
                _('closed_at'):               lambda x: x[_('completed_at')] + random.randint(1,
                                                                                              60),
                _('attempts'):                lambda x: 3,
            },
            'failed':    {
                _('greenfield'):              lambda x: round(time.time()) + _cfg('greenfield_invocation_delta')
                                                        - random.randint(1000, 10000),
                _('labourer_id_task_status'): lambda x: f"{self.LABOURER.id}_0",
                _('closed_at'):               lambda x: x[_('greenfield')] + 900 + random.randint(1, 60),
                _('attempts'):                lambda x: 3,
            },
        }

        # raise ValueError(f"Unsupported `status`: {status}. Should be one of: 'available', 'invoked'.")

        workers = [self.LABOURER.id] if not mutiple_labourers else range(42, 45)
        output = []

        for worker_id in workers:

            for i in range(count_tasks):
                row = {
                    self.HASH_KEY[0]: f"task_id_{worker_id}_{i}_{str(uuid.uuid4())[:8]}",  # Task ID
                }

                for field, getter in MAP[status].items():
                    row[field] = getter(row)

                print(f"Putting {row} to {table}")
                output.append(row)
                safe_put_to_ddb(row, self.dynamo_client, table_name=table)

        return output


    def test_get_next_for_labourer(self):
        self.setup_tasks()

        result = self.manager.get_next_for_labourer(self.LABOURER, only_ids=True)
        # print(result)

        self.assertEqual(len(result), 1, "Returned more than one task")
        self.assertIn(f'task_id_{self.LABOURER.id}_', result[0])


    def test_get_next_for_labourer__multiple(self):
        self.setup_tasks()

        result = self.manager.get_next_for_labourer(self.LABOURER, cnt=5000, only_ids=True)
        # print(result)

        self.assertEqual(len(result), 3, "Should be just 3 tasks for this worker in setup")
        self.assertTrue(all(f'task_id_{self.LABOURER.id}_' in task for task in result),
                        "Returned some tasks of other Workers")


    def test_get_next_for_labourer__not_take_invoked(self):
        self.setup_tasks()
        self.setup_tasks(status='invoked')

        result = self.manager.get_next_for_labourer(self.LABOURER, cnt=50, only_ids=True)
        # print(result)

        self.assertEqual(len(result), 3, "Should be just 3 tasks for this worker in setup. The other 3 are invoked.")
        self.assertTrue(all(f'task_id_{self.LABOURER.id}_' in task for task in result),
                        "Returned some tasks of other Workers")


    def test_get_next_for_labourer__full_tasks(self):
        self.setup_tasks()

        result = self.manager.get_next_for_labourer(self.LABOURER, cnt=2)
        # print(result)

        self.assertEqual(len(result), 2, "Should be just 2 tasks as requested")

        for task in result:
            self.assertIn(f'task_id_{self.LABOURER.id}_', task['task_id']), "Returned some tasks of other Workers"
            self.assertEqual(self.LABOURER.id, task['labourer_id']), "Returned some tasks of other Workers"


    def register_labourers(self):
        self.manager.get_labourers = MagicMock(return_value=[self.LABOURER])
        return self.manager.register_labourers()


    def test_mark_task_invoked(self):
        greenfield = 1000
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
        with patch('time.time') as mock_time:
            mock_time.return_value = self.NOW_TIME
            self.manager.mark_task_invoked(self.LABOURER, row)

        result = self.dynamo_client.get_by_query({self.HASH_KEY[0]: f"task_id_{self.LABOURER.id}_256"},
                                                 fetch_all_fields=True)
        # print(f"The new updated value of task is: {result}")

        # Rounded -2 we check that the greenfield was updated
        self.assertAlmostEqual(self.NOW_TIME + delta, result[0]['greenfield'])


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
        tasks = self.dynamo_client.get_by_query({_('task_id'): '123'})
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

        # Call
        with patch('time.time') as mock_time:
            mock_time.return_value = self.NOW_TIME
            self.manager.move_task_to_retry_table(task, delay)

            result_tasks = self.dynamo_client.get_by_query({_('task_id'): '123'})
            self.assertEqual(len(result_tasks), 0)

            result_retry_tasks = self.dynamo_client.get_by_query({_('labourer_id'): labourer_id},
                                                                 table_name=self.retry_tasks_table)
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

        tasks = RETRY_TASKS.copy()
        # Add tasks to retry table
        for task in tasks:
            self.dynamo_client.put(task, self.config['sosw_retry_tasks_table'])

        # Call
        with patch('time.time') as t:
            t.return_value = 9500
            labourer = self.manager.register_labourers()[0]

        result_tasks = self.manager.get_tasks_to_retry_for_labourer(labourer, limit=20)

        self.assertEqual(len(result_tasks), 2)

        # Check it only gets tasks with timestamp <= now
        self.assertIn(tasks[0], result_tasks)
        self.assertIn(tasks[1], result_tasks)


    @unittest.skip("This funciton moved to Scavenger")
    def test_retry_tasks(self):
        _ = self.manager.get_db_field_name

        with patch('time.time') as t:
            t.return_value = 9500
            labourer = self.manager.register_labourers()[0]

        self.manager.get_oldest_greenfield_for_labourer = Mock(return_value=8888)

        # Add tasks to tasks_table
        regular_tasks = [
            {
                _('labourer_id'): labourer.id, _('task_id'): '11', _('arn'): 'some_arn', _('payload'): {},
                _('greenfield'):  8888
            },
            {
                _('labourer_id'): labourer.id, _('task_id'): '22', _('arn'): 'some_arn', _('payload'): {},
                _('greenfield'):  9999
            },
        ]
        for task in regular_tasks:
            self.dynamo_client.put(task)

        # Add tasks to retry_table
        retry_tasks = RETRY_TASKS.copy()

        for task in retry_tasks:
            self.dynamo_client.put(task, table_name=self.config['sosw_retry_tasks_table'])

        retry_table_items = self.dynamo_client.get_by_scan(table_name=self.retry_tasks_table)
        self.assertEqual(len(retry_table_items), len(retry_tasks))

        # Use get_tasks_to_retry_for_labourer to get tasks
        tasks = self.manager.get_tasks_to_retry_for_labourer(labourer)

        # Call
        self.manager.retry_tasks(labourer, tasks)

        # Check removed 2 out of 3 tasks from retry queue. One is desired to be launched later.
        retry_table_items = self.dynamo_client.get_by_scan(table_name=self.retry_tasks_table)
        self.assertEqual(len(retry_table_items), 1)

        # Check tasks moved to `tasks_table` with lowest greenfields
        tasks_table_items = self.dynamo_client.get_by_scan()
        for x in tasks_table_items:
            print(x)
        self.assertEqual(len(tasks_table_items), 4)

        for reg_task in regular_tasks:
            self.assertIn(reg_task, tasks_table_items)

        for retry_task in retry_tasks:
            try:
                matching = next(x for x in tasks_table_items if x[_('task_id')] == retry_task[_('task_id')])
            except StopIteration:
                print(f"Task not retried {retry_task}. Probably not yet desired.")
                continue

            for k in retry_task.keys():
                if k not in [_('greenfield'), _('desired_launch_time')]:
                    self.assertEqual(retry_task[k], matching[k])

            for k in matching.keys():
                if k != _('greenfield'):
                    self.assertEqual(retry_task[k], matching[k])

            print(f"New greenfield of a retried task: {matching[_('greenfield')]}")
            self.assertTrue(matching[_('greenfield')] < min([x[_('greenfield')] for x in regular_tasks]))


    @unittest.skip("This function moved to Scavenger")
    @patch.object(boto3, '__version__', return_value='1.9.53')
    def test_retry_tasks__old_boto(self, n):
        self.test_retry_tasks()


    def test_get_oldest_greenfield_for_labourer__get_newest_greenfield_for_labourer(self):
        with patch('time.time') as t:
            t.return_value = 9500
            labourer = self.manager.register_labourers()[0]

        min_gf = 20000
        max_gf = 10000
        for i in range(5):  # Ran this with range(1000), it passes :)
            gf = random.randint(10000, 20000)
            if gf < min_gf:
                min_gf = gf
            if gf > max_gf:
                max_gf = gf
            row = {'labourer_id': f"{labourer.id}", 'task_id': f"task-{i}", 'greenfield': gf}
            safe_put_to_ddb(row, self.dynamo_client)

        result = self.manager.get_oldest_greenfield_for_labourer(labourer)
        self.assertEqual(min_gf, result)

        newest = self.manager.get_newest_greenfield_for_labourer(labourer)
        self.assertEqual(max_gf, newest)


    def test_get_length_of_queue_for_labourer(self):
        labourer = Labourer(id='some_lambda', arn='some_arn')

        num_of_tasks = 3  # Ran this with 464 tasks and it worked

        for i in range(num_of_tasks):
            row = {'labourer_id': f"some_lambda", 'task_id': f"task-{i}", 'greenfield': i}
            safe_put_to_ddb(row, self.dynamo_client)

        queue_len = self.manager.get_length_of_queue_for_labourer(labourer)

        self.assertEqual(queue_len, num_of_tasks)


    def test_get_average_labourer_duration__calculates_average__only_failing_tasks(self):
        self.manager.ecology_client.get_max_labourer_duration.return_value = 900
        some_labourer = self.register_labourers()[0]

        self.setup_tasks(status='failed', count_tasks=15)

        self.assertEqual(900, self.manager.get_average_labourer_duration(some_labourer))


    def test_get_average_labourer_duration__calculates_average(self):
        self.manager.ecology_client.get_max_labourer_duration.return_value = 900
        some_labourer = self.register_labourers()[0]

        self.setup_tasks(status='closed', count_tasks=15)
        self.setup_tasks(status='failed', count_tasks=15)

        self.assertLessEqual(self.manager.get_average_labourer_duration(some_labourer), 900)
        self.assertGreaterEqual(self.manager.get_average_labourer_duration(some_labourer), 10)


    def test_get_task_by_id__check_return_task_with_all_attrs(self):
        tasks = self.setup_tasks()
        result = self.manager.get_task_by_id(tasks[0]['task_id'])
        self.assertEqual(result, tasks[0])
