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
    BOTO_RESPONSE = {
        'ResponseMetadata': {
            'RequestId': 'be3ddba3-70e4-11e9-9f5d-31d0e015bfe2',
            'HTTPStatusCode': 200,
            'HTTPHeaders': {
                'date': 'Tue, 07 May 2019 16:25:35 GMT',
                'content-type': 'application/json',
                'content-length': '816',
                'connection': 'keep-alive',
                'x-amzn-requestid': 'be3ddba3-70e4-11e9-9f5d-31d0e015bfe2',
                },
            'RetryAttempts': 0,
            },
        'FunctionName': 'adw_label_manager',
        'FunctionArn': 'arn:aws:lambda:us-west-2:737060422660:function:adw_label_manager',
        'Runtime': 'python3.6',
        'Role': 'arn:aws:iam::737060422660:role/lambda_adw_label_manager',
        'Handler': 'app.lambda_handler',
        'CodeSize': 13964834,
        'Description': 'ABS. CloudFormation managed. adw_label_manager description.',
        'Timeout': 300,
        'MemorySize': 1536,
        'LastModified': '2019-04-30T08:39:44.125+0000',
        'CodeSha256': '1MjZMvGz1itPA0C4S5t+yUSoWaCKHOHPyXWG9SGQFSk=',
        'Version': '$LATEST',
        'VpcConfig': {'SubnetIds': ['subnet-3e584749', 'subnet-21d9a344'],
                      'SecurityGroupIds': ['sg-51659436'],
                      'VpcId': 'vpc-4a741c2f'},
        'TracingConfig': {'Mode': 'PassThrough'},
        'RevisionId': 'e64c32c3-339a-4443-a7a4-ab2a81d6d5c7',
        }


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


    def test_get_max_labourer_duration(self):
        self.manager.task_client = MagicMock()
        self.manager.task_client.lambda_client.get_function_configuration.return_value = self.BOTO_RESPONSE

        self.assertEqual(self.manager.get_max_labourer_duration(self.LABOURER), 300)
