import json
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
        self.get_config_patch.return_value = {}


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass


    def test_mark_task_as_completed__called(self):
        with patch('boto3.client'):
            p = Worker()

        p.mark_task_as_completed = MagicMock(return_value=None)

        p({'task_id': '123'})
        p.mark_task_as_completed.assert_called_once_with('123')


    def test_init__meta_handler_config__initializes_meta_handler(self):
        with patch('boto3.client'), patch('sosw.worker.MetaHandler') as meta_handler_mock:
            p = Worker(custom_config={'meta_handler_config': {'init_clients': []}})

        meta_handler_mock.assert_called_once_with(custom_config={'init_clients': []})
        self.assertIs(p.meta_handler, meta_handler_mock.return_value)


    def test_init__no_meta_handler_config__meta_handler_not_initialized(self):
        with patch('boto3.client'):
            p = Worker()

        self.assertIsNone(p.meta_handler)


    def test_call__no_task_id__does_not_call_mark_task_as_completed(self):
        with patch('boto3.client'):
            p = Worker()

        p.mark_task_as_completed = MagicMock()

        p({'pay': 'load'})

        p.mark_task_as_completed.assert_not_called()
        self.assertEqual(p.stats['processor_calls'], 1)


    def test_call__mark_task_as_completed_raises__still_calls_processor(self):
        with patch('boto3.client'):
            p = Worker()

        p.mark_task_as_completed = MagicMock(side_effect=Exception("Boom"))

        p({'task_id': '123'})

        p.mark_task_as_completed.assert_called_once_with('123')
        self.assertEqual(p.stats['processor_calls'], 1)


    def test_mark_task_as_completed__payload_minimal(self):
        with patch('boto3.client'):
            p = Worker()

        p.stats = {}

        p.mark_task_as_completed('123')

        call_kwargs = p.lambda_client.invoke.call_args.kwargs
        self.assertEqual(call_kwargs['FunctionName'], 'sosw_worker_assistant')
        self.assertEqual(call_kwargs['InvocationType'], 'Event')
        self.assertEqual(json.loads(call_kwargs['Payload']), {'action': 'mark_task_as_completed', 'task_id': '123'})


    def test_mark_task_as_completed__payload_with_stats_and_result(self):
        with patch('boto3.client'):
            p = Worker(custom_config={'sosw_worker_assistant_lambda': 'custom_assistant'})

        p.meta_handler = MagicMock()
        p.stats = {'lines_parsed': 10}
        p.result = {'rows_written': 5}

        p.mark_task_as_completed('123')

        call_kwargs = p.lambda_client.invoke.call_args.kwargs
        self.assertEqual(call_kwargs['FunctionName'], 'custom_assistant')
        self.assertEqual(json.loads(call_kwargs['Payload']),
                         {'action': 'mark_task_as_completed', 'task_id': '123',
                          'stats': {'lines_parsed': 10}, 'result': {'rows_written': 5}})
        p.meta_handler.post.assert_called_once_with(task_id='123', action='completed')


    def test_mark_task_as_completed__registers_lambda_client_if_missing(self):
        with patch('boto3.client'):
            p = Worker()

        p.lambda_client = None

        with patch('boto3.client') as client_mock:
            p.mark_task_as_completed('123')

        client_mock.assert_called_once_with('lambda')
        p.lambda_client.invoke.assert_called_once()


    def test_mark_task_as_failed__payload_minimal(self):
        with patch('boto3.client'):
            p = Worker()

        p.stats = {}

        p.mark_task_as_failed('123')

        call_kwargs = p.lambda_client.invoke.call_args.kwargs
        self.assertEqual(call_kwargs['FunctionName'], 'sosw_worker_assistant')
        self.assertEqual(call_kwargs['InvocationType'], 'Event')
        self.assertEqual(json.loads(call_kwargs['Payload']), {'action': 'mark_task_as_failed', 'task_id': '123'})


    def test_mark_task_as_failed__payload_with_stats_and_result(self):
        with patch('boto3.client'):
            p = Worker(custom_config={'sosw_worker_assistant_lambda': 'custom_assistant'})

        p.meta_handler = MagicMock()
        p.stats = {'lines_parsed': 10}
        p.result = {'rows_written': 5}

        p.mark_task_as_failed('123')

        call_kwargs = p.lambda_client.invoke.call_args.kwargs
        self.assertEqual(call_kwargs['FunctionName'], 'custom_assistant')
        self.assertEqual(json.loads(call_kwargs['Payload']),
                         {'action': 'mark_task_as_failed', 'task_id': '123',
                          'stats': {'lines_parsed': 10}, 'result': {'rows_written': 5}})
        p.meta_handler.post.assert_called_once_with(task_id='123', action='failed')


    def test_mark_task_as_failed__registers_lambda_client_if_missing(self):
        with patch('boto3.client'):
            p = Worker()

        p.lambda_client = None

        with patch('boto3.client') as client_mock:
            p.mark_task_as_failed('123')

        client_mock.assert_called_once_with('lambda')
        p.lambda_client.invoke.assert_called_once()
