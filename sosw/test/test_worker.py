import boto3
import os
import unittest

from unittest import mock
from unittest.mock import MagicMock, patch

from sosw.worker import Worker


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class worker_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = {'test': True}


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except:
            pass


    @mock.patch("sosw.worker.close_task")
    def test_close_task__called(self, mock_close_task):
        p = Worker(custom_config=self.TEST_CONFIG)
        p({'key': 'payload'})

        mock_close_task.assert_called_once_with({'key': 'payload'})
