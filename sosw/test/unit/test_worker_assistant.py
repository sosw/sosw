import os
import time
import unittest
from unittest.mock import patch, Mock, MagicMock


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


    def test_call__stats_and_result_as_dict__passed_as_is(self):
        event = {
            'action':  'mark_task_as_completed',
            'task_id': '123',
            'stats':   {'s_key': 'value'},
            'result':  {'r_key': 'value'}
        }

        self.worker_assistant.mark_task_as_completed = Mock(return_value=None)
        self.worker_assistant(event)
        self.worker_assistant.mark_task_as_completed.assert_called_once_with(task_id='123', stats={'s_key': 'value'},
                                                                             result={'r_key': 'value'})


    def test_call__mark_task_as_failed(self):
        event = {
            'action':  'mark_task_as_failed',
            'task_id': '123'
        }

        self.worker_assistant.mark_task_as_failed = Mock(return_value=None)
        self.worker_assistant(event)
        self.worker_assistant.mark_task_as_failed.assert_called_once_with(task_id='123')


    def test_mark_task_as_completed__updates_task_and_posts_meta(self):
        self.worker_assistant.dynamo_db_client = MagicMock()
        self.worker_assistant.meta_handler = MagicMock()

        self.worker_assistant.mark_task_as_completed('123', stats={'lines_parsed': 10}, result={'rows_written': 5})

        call_kwargs = self.worker_assistant.dynamo_db_client.update.call_args.kwargs
        self.assertEqual(call_kwargs['keys'], {'task_id': '123'})

        attributes = call_kwargs['attributes_to_update']
        self.assertEqual(attributes['stat_lines_parsed'], 10)
        self.assertEqual(attributes['result_rows_written'], 5)
        self.assertAlmostEqual(attributes['completed_at'], int(time.time()), delta=5)

        self.worker_assistant.meta_handler.post.assert_called_once_with(task_id='123', action='marked_as_completed')


    def test_mark_task_as_completed__no_stats_no_result__updates_only_completed_at(self):
        self.worker_assistant.dynamo_db_client = MagicMock()
        self.worker_assistant.meta_handler = MagicMock()

        self.worker_assistant.mark_task_as_completed('123')

        call_kwargs = self.worker_assistant.dynamo_db_client.update.call_args.kwargs
        self.assertEqual(list(call_kwargs['attributes_to_update'].keys()), ['completed_at'])


    def test_mark_task_as_completed__non_string_task_id__raises(self):
        with self.assertRaises(AssertionError):
            self.worker_assistant.mark_task_as_completed(123)


    def test_mark_task_as_failed__increments_failed_attempts(self):
        self.worker_assistant.dynamo_db_client = MagicMock()
        self.worker_assistant.meta_handler = MagicMock()

        self.worker_assistant.mark_task_as_failed('123', stats={'lines_parsed': 10}, result={'rows_written': 5})

        call_kwargs = self.worker_assistant.dynamo_db_client.update.call_args.kwargs
        self.assertEqual(call_kwargs['keys'], {'task_id': '123'})
        self.assertEqual(call_kwargs['attributes_to_increment'], {'failed_attempts': 1})
        self.assertEqual(call_kwargs['attributes_to_update'], {'stat_lines_parsed': 10, 'result_rows_written': 5})

        self.worker_assistant.meta_handler.post.assert_called_once_with(task_id='123', action='marked_as_failed')


    def test_mark_task_as_failed__no_stats_no_result__does_not_update_attributes(self):
        self.worker_assistant.dynamo_db_client = MagicMock()
        self.worker_assistant.meta_handler = MagicMock()

        self.worker_assistant.mark_task_as_failed('123')

        call_kwargs = self.worker_assistant.dynamo_db_client.update.call_args.kwargs
        self.assertNotIn('attributes_to_update', call_kwargs)
        self.assertEqual(call_kwargs['attributes_to_increment'], {'failed_attempts': 1})


    def test_mark_task_as_failed__non_string_task_id__raises(self):
        with self.assertRaises(AssertionError):
            self.worker_assistant.mark_task_as_failed(123)


    def test_get_db_field_name(self):
        self.assertEqual(self.worker_assistant.get_db_field_name('task_id'), 'task_id')

        self.worker_assistant.config['dynamo_db_config']['field_names'] = {'task_id': 'custom_task_id'}
        self.assertEqual(self.worker_assistant.get_db_field_name('task_id'), 'custom_task_id')
        self.assertEqual(self.worker_assistant.get_db_field_name('unmapped_field'), 'unmapped_field')
