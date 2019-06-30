import os
import unittest

from unittest.mock import patch, MagicMock

from sosw.worker import Worker


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Worker_UnitTestCase(unittest.TestCase):


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass


    def test_close_task__called(self):
        with patch('boto3.client'):
            p = Worker()

        p.mark_task_as_completed = MagicMock(return_value=None)

        p({'task_id': '123'})
        p.mark_task_as_completed.assert_called_once_with('123')
