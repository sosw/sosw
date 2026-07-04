import os
import unittest
from copy import deepcopy

from unittest.mock import Mock, MagicMock, patch, call

from sosw.scavenger import Scavenger
from sosw.labourer import Labourer
from sosw.managers.meta_handler import MetaHandler
from sosw.test.variables import TEST_SCAVENGER_CONFIG, TASKS, LABOURERS


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Scavenger_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_SCAVENGER_CONFIG


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()
        self.get_config_patch.return_value = {}
        self.custom_config = deepcopy(self.TEST_CONFIG)

        with patch('boto3.client'):
            self.scavenger = Scavenger(custom_config=self.custom_config)

        # Mock clients
        self.scavenger.task_client = MagicMock()
        self.scavenger.ecology_client = MagicMock()
        self.scavenger.sns_client = MagicMock()
        self.scavenger.meta_handler = MagicMock(signature=MetaHandler)

        self.scavenger.get_db_field_name = MagicMock(side_effect=lambda x: x)
        _ = self.scavenger.get_db_field_name

        self.labourer = Labourer(id='lambda3', arn='arn3')
        self.task = {
            _('task_id'): '123', _('labourer_id'): 'lambda1', _('greenfield'): '3525624', _('payload'): '{"a": 1}',
            _('attempts'): 2
        }


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass


    def test_call(self):
        # Mock
        self.scavenger.task_client.register_labourers = Mock(return_value=LABOURERS)
        self.scavenger.handle_expired_tasks = Mock()
        self.scavenger.archive_tasks = Mock()
        self.scavenger.retry_tasks = Mock()

        # Call
        self.scavenger()

        # Check call
        self.assertEqual(self.scavenger.handle_expired_tasks.call_count, 3)
        self.assertEqual(self.scavenger.archive_tasks.call_count, 3)
        self.assertEqual(self.scavenger.retry_tasks.call_count, 3)


    def test_handle_expired_tasks_for_labourer(self):
        labourer = LABOURERS[1]
        expired_tasks_per_lambda = {
            'some_lambda':    [TASKS[0]],
            'another_lambda': [TASKS[1], TASKS[2]]
        }

        self.scavenger.task_client.get_expired_tasks_for_labourer = MagicMock(
                side_effect=lambda l: expired_tasks_per_lambda.get(l.id, []))
        self.scavenger.process_expired_task = Mock()

        # Call
        self.scavenger.handle_expired_tasks(labourer)

        # Check call
        self.scavenger.task_client.get_expired_tasks_for_labourer.assert_called_once_with(labourer)

        self.scavenger.process_expired_task.assert_has_calls(
                [call(labourer, TASKS[1]), call(labourer, TASKS[2])]
        )


    def test_process_expired_task__close(self):
        # Mock
        self.scavenger.should_retry_task = Mock(return_value=False)
        self.scavenger.move_task_to_retry_table = Mock()
        self.scavenger.task_client.archive_task = Mock()

        # Call
        self.scavenger.process_expired_task(self.labourer, self.task)

        # Check mock calls
        self.scavenger.task_client.archive_task.assert_called_once_with('123')
        self.scavenger.task_client.move_task_to_retry_table.assert_not_called()


    def test_process_expired_task__dont_close(self):
        # Mock
        self.scavenger.should_retry_task = Mock(return_value=True)
        self.scavenger.move_task_to_retry_table = Mock()
        self.scavenger.task_client.archive_task = Mock()

        # Call
        self.scavenger.process_expired_task(self.labourer, self.task)

        # Check mock calls
        self.scavenger.move_task_to_retry_table.assert_called_once_with(self.task, self.labourer)
        self.scavenger.task_client.archive_task.assert_not_called()


    def test_calculate_delay_for_task_retry(self):
        _ = self.scavenger.get_db_field_name
        labourer = Labourer(id='some_lambda', arn='some_arn', max_duration=45)
        task = {_('task_id'): '123', _('labourer_id'): 'some_lambda', _('payload'): '{}', _('attempts'): 5}
        result = self.scavenger.calculate_delay_for_task_retry(labourer, task)
        self.assertEqual(result, 225)


    def test_should_retry_task(self):
        labourer = Labourer(id='some_lambda', arn='some_arn', max_attempts=3)

        self.assertTrue(self.scavenger.should_retry_task(labourer, {'attempts': 2}))
        self.assertFalse(self.scavenger.should_retry_task(labourer, {'attempts': 3}))
        self.assertFalse(self.scavenger.should_retry_task(labourer, {'attempts': 4}))


    def test_move_task_to_retry_table(self):
        labourer = Labourer(id='lambda1', arn='arn1', max_duration=300)

        self.scavenger.move_task_to_retry_table(self.task, labourer)

        # Wanted delay: max_duration 300 * attempts 2 = 600.
        self.scavenger.task_client.move_task_to_retry_table.assert_called_once_with(self.task, 600)
        self.scavenger.meta_handler.post.assert_called_once_with(task_id='123', labourer_id='lambda1',
                                                                 action='scheduled_for_retry')


    def test_retry_tasks(self):
        tasks_to_retry = [
            {'task_id': '511', 'labourer_id': 'lambda3'},
            {'task_id': '512', 'labourer_id': 'lambda3'},
        ]
        self.scavenger.task_client.get_tasks_to_retry_for_labourer = MagicMock(return_value=tasks_to_retry)
        self.scavenger.task_client.get_oldest_greenfield_for_labourer = MagicMock(return_value=1000)

        self.scavenger.retry_tasks(self.labourer)

        self.scavenger.task_client.get_tasks_to_retry_for_labourer.assert_called_once_with(labourer=self.labourer,
                                                                                           limit=20)
        self.scavenger.task_client.retry_task.assert_has_calls([
            call(task=tasks_to_retry[0], labourer_id='lambda3', greenfield=999),
            call(task=tasks_to_retry[1], labourer_id='lambda3', greenfield=998),
        ])
        self.scavenger.meta_handler.post.assert_has_calls([
            call(task_id='511', labourer_id='lambda3', action='ready_for_retry'),
            call(task_id='512', labourer_id='lambda3', action='ready_for_retry'),
        ])


    def test_retry_tasks__no_tasks(self):
        self.scavenger.task_client.get_tasks_to_retry_for_labourer = MagicMock(return_value=[])
        self.scavenger.task_client.get_oldest_greenfield_for_labourer = MagicMock(return_value=1000)

        self.scavenger.retry_tasks(self.labourer)

        self.scavenger.task_client.retry_task.assert_not_called()
        self.scavenger.meta_handler.post.assert_not_called()


    def test_archive_tasks(self):
        completed_tasks = [
            {'task_id': '123', 'labourer_id': 'lambda3'},
            {'task_id': '124', 'labourer_id': 'lambda3'},
        ]
        self.scavenger.task_client.get_completed_tasks_for_labourer = MagicMock(return_value=completed_tasks)

        self.scavenger.archive_tasks(self.labourer)

        self.scavenger.task_client.archive_task.assert_has_calls([call('123'), call('124')])
        self.scavenger.meta_handler.post.assert_has_calls([
            call(task_id='123', labourer_id='lambda3', action='archived'),
            call(task_id='124', labourer_id='lambda3', action='archived'),
        ])


    def test_get_db_field_name(self):
        # Remove the identity mock installed by setUp to reach the real method.
        del self.scavenger.get_db_field_name

        self.scavenger.task_client.get_db_field_name = MagicMock(return_value='mapped_field')

        self.assertEqual(self.scavenger.get_db_field_name('attempts'), 'mapped_field')
        self.scavenger.task_client.get_db_field_name.assert_called_once_with('attempts')
