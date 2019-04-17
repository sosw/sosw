import os
import unittest

from unittest.mock import patch, Mock

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


    def test_close_task__called(self):
        p = Worker(custom_config=self.TEST_CONFIG)
        p.mark_task_as_completed = Mock(return_value=None)
        p({'task_id': '123'})
        p.mark_task_as_completed.assert_called_once_with('123')
