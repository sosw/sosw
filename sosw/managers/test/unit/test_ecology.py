import boto3
import datetime
import logging
import time
import unittest
import os

from copy import deepcopy
from dateutil.tz import tzlocal
from unittest.mock import MagicMock, patch

from sosw.components.helpers import make_hash


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.labourer import Labourer
from sosw.managers.ecology import EcologyManager
from sosw.test.variables import TEST_ECOLOGY_CLIENT_CONFIG


class ecology_manager_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_ECOLOGY_CLIENT_CONFIG
    LABOURER = Labourer(id='some_function', arn='arn:aws:lambda:us-west-2:000000000000:function:some_function')
    SAMPLE_HEALTH_METRICS = {
        'test1': {'details': {'Name': 'CPUUtilization', 'Namespace': 'AWS/RDS'}},
        'test2': {'details': {'Name': 'CPUUtilization2', 'Namespace': 'AWS/RDS'}},
        'test3': {'details': {'Name': 'CPUUtilization3', 'Namespace': 'AWS/RDS'}},
    }

    SAMPLE_GET_METRICS_STATISTICS_RESPONSE = {
        'Label':            'CPUUtilization',
        'Datapoints':       [{
                                 'Timestamp': datetime.datetime(2019, 5, 13, 14, 3, tzinfo=tzlocal()),
                                 'Average':   31.3333333345751,
                                 'Unit':      'Percent'
                             },
                             {
                                 'Timestamp': datetime.datetime(2019, 5, 13, 14, 0, tzinfo=tzlocal()),
                                 'Average':   100.0,
                                 'Unit':      'Percent'
                             },
                             {
                                 'Timestamp': datetime.datetime(2019, 5, 13, 14, 4, tzinfo=tzlocal()),
                                 'Average':   29.4999999987582,
                                 'Unit':      'Percent'
                             },
        ],
        'ResponseMetadata': {
            'HTTPStatusCode': 200,
        }
    }


    def setUp(self):
        """
        We keep copies of main parameters here, because they may differ from test to test and cleanup needs them.
        This is responsibility of the test author to update these values if required from test.
        """

        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.config = self.TEST_CONFIG.copy()

        with patch('boto3.client'):
            self.manager = EcologyManager(custom_config=self.config)


    def tearDown(self):
        self.patcher.stop()


    def test_eco_statuses(self):
        self.assertEqual(set(self.manager.eco_statuses), set(range(5)))


    def test_count_running_tasks_for_labourer__raises_not_task_client(self):
        self.assertRaises(RuntimeError, self.manager.count_running_tasks_for_labourer, self.LABOURER)


    def test_count_running_tasks_for_labourer__use_local_cache(self):
        tm = MagicMock()
        self.manager.register_task_manager(tm)

        # Setting something to local cache.
        self.manager.running_tasks[self.LABOURER.id] = 5

        self.assertEqual(self.manager.count_running_tasks_for_labourer(self.LABOURER), 5)
        self.manager.task_client.get_count_of_running_tasks_for_labourer.assert_not_called()


    def test_count_running_tasks_for_labourer__calls_task_manager(self):
        tm = MagicMock()
        tm.get_count_of_running_tasks_for_labourer.return_value = 42
        self.manager.register_task_manager(tm)

        self.assertEqual(self.manager.count_running_tasks_for_labourer(self.LABOURER), 42)
        self.manager.task_client.get_count_of_running_tasks_for_labourer.assert_called_once()


    def test_register_task_manager__resets_stats(self):
        # Should be defaultdict(int)
        self.assertEqual(self.manager.running_tasks['foo'], 0)

        # Manually increase counter
        self.manager.running_tasks['foo'] += 10
        self.assertEqual(self.manager.running_tasks['foo'], 10)

        # Call register_task_manager
        self.manager.register_task_manager(MagicMock())
        self.assertEqual(self.manager.running_tasks['foo'], 0, "Did not reset cache of running_tasks")


    def test_add_running_tasks_for_labourer(self):
        tm = MagicMock()
        tm.get_count_of_running_tasks_for_labourer.return_value = 12
        self.manager.register_task_manager(tm)

        # Not yet cached
        self.assertNotIn(self.LABOURER.id, self.manager.running_tasks.keys())

        # Add default number
        self.manager.add_running_tasks_for_labourer(labourer=self.LABOURER)

        # Should have been called first time to cache info about this Labourer.
        self.manager.task_client.get_count_of_running_tasks_for_labourer.assert_called_once()

        # Make sure the cache is fetched and increased by the counter
        self.assertEqual(self.manager.running_tasks[self.LABOURER.id],
                         tm.get_count_of_running_tasks_for_labourer.return_value + 1)

        # Call again to add 5 more tasks
        self.manager.add_running_tasks_for_labourer(labourer=self.LABOURER, count=5)

        # The counter of the task manager should not have been increased.
        self.manager.task_client.get_count_of_running_tasks_for_labourer.assert_called_once()

        # But the counter of tasks in cache should have.
        self.assertEqual(self.manager.running_tasks[self.LABOURER.id],
                         tm.get_count_of_running_tasks_for_labourer.return_value + 1 + 5)


    def test_get_max_labourer_duration(self):
        self.manager.task_client = MagicMock()
        self.manager.task_client.lambda_client.get_function_configuration.return_value = {'Timeout': 300}

        self.assertEqual(self.manager.get_max_labourer_duration(self.LABOURER), 300)


    def test_get_health(self):
        METRIC = {
            'details':                     {},
            'feelings':                    {
                3: 50,
                4: 25,
            },
            'feeling_comparison_operator': '__le__'
        }

        TESTS = [
            (0, 4),
            (1.0, 4),
            (25, 4),
            (25.000001, 3),
            (30, 3),
            (50, 3),
            (51, 0),
        ]

        for value, expected in TESTS:
            self.assertEqual(self.manager.get_health(value, METRIC), expected, f"Failed: {value} t")


    def test_get_health__invalid(self):
        METRIC = {
            'details':                     {},
            'feelings':                    {
                1: 40,
                3: 50,
                4: 25,
            },
            'feeling_comparison_operator': '__le__'
        }

        self.assertRaises(ValueError, self.manager.get_health, 60, METRIC), \
        "Did not raise while the feelings are invalid. Order of values should respect order of health statuses."


    def test_get_labourer_status(self):
        self.manager.get_health = MagicMock(side_effect=[3, 2, 4])
        self.manager.register_task_manager(MagicMock())
        self.manager.fetch_metric_stats = MagicMock()
        self.health_metrics = dict()

        labourer = deepcopy(self.LABOURER)
        setattr(labourer, 'health_metrics', self.SAMPLE_HEALTH_METRICS)

        # Calling the actual tested method.
        result = self.manager.get_labourer_status(labourer)

        # The result should be the lowest of values get_health would have returned out of three calls.
        self.assertEqual(result, 2, f"Did not get the lowest health result. Received: {result}")

        # Chech the the get_health had been called three times (for each metric).
        self.manager.get_health.assert_called()
        self.assertEqual(self.manager.get_health.call_count, 3)

        self.manager.fetch_metric_stats.assert_called()
        self.assertEqual(self.manager.fetch_metric_stats.call_count, 3)


    def test_get_labourer_status__uses_cache(self):
        self.manager.get_health = MagicMock(return_value=0)
        self.manager.register_task_manager(MagicMock())
        self.manager.fetch_metric_stats = MagicMock()

        labourer = deepcopy(self.LABOURER)
        setattr(labourer, 'health_metrics', self.SAMPLE_HEALTH_METRICS)

        self.manager.health_metrics = {make_hash(labourer.health_metrics['test1']['details']): 42}

        # Calling the actual tested method.
        result = self.manager.get_labourer_status(labourer)

        # Assert calculator (get_health) was called 3 times.
        self.assertEqual(self.manager.get_health.call_count, 3)
        self.assertEqual(self.manager.fetch_metric_stats.call_count, 2,
                         f"Fetcher was supposed to be called only for 2 metrics. One is in cache.")


    def test_fetch_metric_stats__calls_boto(self):
        self.manager.cloudwatch_client = MagicMock()
        self.manager.cloudwatch_client.get_metric_statistics.return_value = self.SAMPLE_GET_METRICS_STATISTICS_RESPONSE
        self.manager.fetch_metric_stats(metric={'a': 1, 'b': {3: 42}})

        self.manager.cloudwatch_client.get_metric_statistics.assert_called_once()


    def test_fetch_metric_stats__calculates_time(self):
        MOCK_DATE = datetime.datetime(2019, 1, 1, 0, 42, 0)
        self.manager.cloudwatch_client = MagicMock()
        self.manager.cloudwatch_client.get_metric_statistics.return_value = self.SAMPLE_GET_METRICS_STATISTICS_RESPONSE

        with patch('datetime.datetime') as t:
            t.now.return_value = MOCK_DATE
            self.manager.fetch_metric_stats(metric={'a': 1, 'b': {3: 42}})

        args, kwargs = self.manager.cloudwatch_client.get_metric_statistics.call_args
        # print(kwargs)

        self.assertEqual(kwargs['EndTime'], MOCK_DATE)
        self.assertEqual(kwargs['StartTime'],
                         MOCK_DATE - datetime.timedelta(
                                 seconds=self.manager.config['default_metric_values']['MetricAggregationTimeSlice']))


    def test_fetch_metric_stats__use_defaults(self):
        self.manager.cloudwatch_client = MagicMock()
        self.manager.cloudwatch_client.get_metric_statistics.return_value = self.SAMPLE_GET_METRICS_STATISTICS_RESPONSE

        self.manager.fetch_metric_stats(metric={'a': 1})

        _, kwargs = self.manager.cloudwatch_client.get_metric_statistics.call_args

        # Checking some default from hardcoded DEFAULT_CONFIG
        self.assertEqual(kwargs['Period'], self.manager.config['default_metric_values']['Period'])
