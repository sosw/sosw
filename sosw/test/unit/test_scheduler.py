import boto3
import os
import unittest

from unittest import mock
from unittest.mock import MagicMock, patch

from sosw.scheduler import Scheduler
from sosw.labourer import Labourer
from sosw.test.variables import TEST_SCHEDULER_CONFIG


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Scheduler_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_SCHEDULER_CONFIG


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.custom_config = self.TEST_CONFIG.copy()
        self.scheduler = Scheduler(self.custom_config)


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except:
            pass


    def test__queue_bucket(self):
        self.assertEqual(self.scheduler._queue_bucket, self.scheduler.config['queue_bucket'])


    def test__local_queue_file(self):
        self.assertEqual(self.scheduler._local_queue_file, f"/tmp/{self.scheduler.config['queue_file']}")


    def test__remote_queue_file(self):
        self.assertEqual(self.scheduler._remote_queue_file,
                         f"{self.scheduler.config['s3_prefix'].strip('/')}/"
                         f"{self.scheduler.config['queue_file'].strip('/')}")


    def test__remote_queue_locked_file(self):
        self.assertEqual(self.scheduler._remote_queue_locked_file,
                         f"{self.scheduler.config['s3_prefix'].strip('/')}/locked_"
                         f"{self.scheduler.config['queue_file'].strip('/')}")
