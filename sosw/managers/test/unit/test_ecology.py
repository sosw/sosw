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

from sosw.labourer import Labourer
from sosw.managers.ecology import EcologyManager
from sosw.test.variables import TEST_ECOLOGY_CLIENT_CONFIG


class ecology_manager_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_ECOLOGY_CLIENT_CONFIG
    LABOURER = Labourer(id='some_function', arn='arn:aws:lambda:us-west-2:000000000000:function:some_function')


    def setUp(self):
        """
        We keep copies of main parameters here, because they may differ from test to test and cleanup needs them.
        This is responsibility of the test author to update these values if required from test.
        """

        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.config = self.TEST_CONFIG.copy()

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
        raise NotImplementedError
