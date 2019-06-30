import os
import unittest
from unittest.mock import patch, Mock


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


from sosw.worker_assistant import WorkerAssistant


class WorkerAssistant_UnitTestCase(unittest.TestCase):

    def setUp(self):
        with patch('boto3.client'):
            self.worker_assistant = WorkerAssistant()


    def test_call__unknown_action__raises(self):
        event = {
            'action': 'unknown_action'
        }
        with self.assertRaises(Exception):
            self.worker_assistant(event)


    def test_call__mark_task_as_closed(self):
        event = {
            'action': 'mark_task_as_completed',
            'task_id': '123',
        }
        self.worker_assistant.mark_task_as_completed = Mock(return_value=None)
        self.worker_assistant(event)
        self.worker_assistant.mark_task_as_completed.assert_called_once_with(task_id='123')


    def test_call__mark_task_as_closed__no_task_id__raises(self):
        event = {
            'action': 'mark_task_as_completed'
        }
        with self.assertRaises(Exception):
            self.worker_assistant(event)
