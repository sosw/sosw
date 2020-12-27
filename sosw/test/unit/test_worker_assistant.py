import os
import unittest
from unittest.mock import patch, Mock


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.worker_assistant import WorkerAssistant
from sosw.test.variables import TEST_WORKER_ASSISTANT_CONFIG


class WorkerAssistant_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_WORKER_ASSISTANT_CONFIG


    def setUp(self):
        with patch('boto3.client'):
            self.worker_assistant = WorkerAssistant(custom_config=self.TEST_CONFIG)


    def test_call__unknown_action__raises(self):
        event = {
            'action': 'unknown_action'
        }
        with self.assertRaises(Exception):
            self.worker_assistant(event)


    def test_call__mark_task_as_closed(self):
        event = {
            'action':  'mark_task_as_completed',
            'task_id': '123',
            'stats': '{"s_key": "value"}',
            'result': '{"r_key": "value"}'
        }

        self.worker_assistant.mark_task_as_completed = Mock(return_value=None)
        self.worker_assistant(event)
        self.worker_assistant.mark_task_as_completed.assert_called_once_with(task_id='123', stats={"s_key": "value"},
                                                                             result={"r_key": "value"})


    def test_call__mark_task_as_closed__no_task_id__raises(self):
        event = {
            'action': 'mark_task_as_completed'
        }
        with self.assertRaises(Exception):
            self.worker_assistant(event)
