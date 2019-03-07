import os
import unittest

from unittest.mock import Mock, MagicMock, patch, call

from sosw.scavenger import Scavenger
from sosw.labourer import Labourer
from sosw.test.variables import TEST_SCAVENGER_CONFIG


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Scavenger_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_SCAVENGER_CONFIG


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.custom_config = self.TEST_CONFIG.copy()
        self.scavenger = Scavenger(self.custom_config)

        # Mock clients
        self.scavenger.task_client = MagicMock()
        self.scavenger.ecology_client = MagicMock()
        self.scavenger.sns_client = MagicMock()

        self.scavenger.get_db_field_name = MagicMock(side_effect=lambda x: x)
        _ = self.scavenger.get_db_field_name

        self.task = {
            _('task_id'): '123', _('labourer_id'): 'lambda1', _('greenfield'): '3525624', _('payload'): '{"a": 1}',
            _('closed'):  False, _('attempts'): 2
        }


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except:
            pass


    def test_call(self):
        labourers = [Labourer(id='some_lambda', arn='some_arn', some_attr='yes'),
                     Labourer(id='another_lambda', arn='another_arn'),
                     Labourer(id='lambda3', arn='arn3')
                     ]

        expired_tasks = [
            {'task_id': '123', 'labourer_id': 'some_lambda', 'attempts': 3, 'greenfield': '123'},
            {'task_id': '124', 'labourer_id': 'another_lambda', 'attempts': 4, 'greenfield': '321'},
            {'task_id': '125', 'labourer_id': 'some_lambda', 'attempts': 3, 'greenfield': '123'}
        ]


        def get_expired_tasks(labourer):
            return {
                'some_lambda':    [expired_tasks[0]],
                'another_lambda': [expired_tasks[1], expired_tasks[2]]
            }.get(labourer.id, [])


        def get_closed_tasks(labourer):
            return {
                'some_lambda':    [expired_tasks[0]],
                'another_lambda': [expired_tasks[1]]
            }.get(labourer.id, [])


        health = 4

        # Mock
        self.scavenger.task_client.get_labourers = Mock(return_value=labourers)
        self.scavenger.ecology_client.get_labourer_status = Mock(return_value=health)
        self.scavenger.task_client.get_expired_tasks_for_labourer = MagicMock(side_effect=get_expired_tasks)
        self.scavenger.task_client.get_closed_tasks_for_labourer = MagicMock(side_effect=get_closed_tasks)
        self.scavenger.process_expired_task = Mock()
        self.scavenger.task_client.archive_task = Mock()

        # Call
        self.scavenger()

        # Check mock calls
        self.scavenger.task_client.get_labourers.assert_called_once_with()

        self.assertEqual(self.scavenger.task_client.get_expired_tasks_for_labourer.call_count, 3)
        self.scavenger.task_client.get_expired_tasks_for_labourer.assert_has_calls([
            call(labourers['some_lambda']), call(labourers['another_lambda']), call(labourers['lambda3'])
        ])

        self.assertEqual(self.scavenger.ecology_client.get_labourer_status.call_count, 2)
        self.scavenger.ecology_client.get_labourer_status.assert_has_calls([
            call(labourers['some_lambda']), call(labourers['another_lambda'])
        ])

        self.assertEqual(self.scavenger.process_expired_task.call_count, 3)
        self.scavenger.process_expired_task.assert_has_calls([
            call(expired_tasks[0], health), call(expired_tasks[1], health), call(expired_tasks[2], health)
        ])

        self.assertEqual(self.scavenger.task_client.get_closed_tasks_for_labourer.call_count, 3)
        self.scavenger.task_client.get_closed_tasks_for_labourer.assert_has_calls([
            call(labourers['some_lambda']), call(labourers['another_lambda']), call(labourers['lambda3'])
        ])

        self.assertEqual(self.scavenger.task_client.archive_task.call_count, 2)
        self.scavenger.task_client.archive_task.assert_has_calls([
            call('123'), call('124')
        ])


    def test_process_expired_task__close(self):
        # Mock
        self.scavenger.should_retry_task = Mock(return_value=False)
        self.scavenger.allow_task_to_retry = Mock()
        self.scavenger.task_client.close_task = Mock()

        # Call
        self.scavenger.process_expired_task(self.task, 3)

        # Check mock calls
        self.scavenger.task_client.close_task.assert_called_once_with('123')
        self.scavenger.allow_task_to_retry.assert_not_called()


    def test_process_expired_task__dont_close(self):
        # Mock
        self.scavenger.should_retry_task = Mock(return_value=True)
        self.scavenger.allow_task_to_retry = Mock()
        self.scavenger.task_client.close_task = Mock()

        # Call
        self.scavenger.process_expired_task(self.task, 3)

        # Check mock calls
        self.scavenger.allow_task_to_retry.assert_called_once_with(self.task)
        self.scavenger.task_client.close_task.assert_not_called()


    @unittest.skip("Logic is not yet final")
    def test_should_retry_task(self):
        raise NotImplementedError
