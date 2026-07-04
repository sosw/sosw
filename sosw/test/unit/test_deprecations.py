"""
Unit tests for the ``sosw`` 3.0.0 deprecation mechanics.

Covers the ``sosw._deprecation`` helper (once-per-entity guard), the ``DeprecationWarning``
emitted on instantiation of every deprecated orchestration entity, and the lazy package
façade in ``sosw/__init__.py`` (PEP 562).
"""

import os
import subprocess
import sys
import types
import unittest
import warnings

from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock, patch


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

import sosw

from sosw._deprecation import MIGRATION_GUIDE_URL, reset_warned_entities, warn_deprecated
from sosw.app import LambdaGlobals, Processor, global_vars
from sosw.essential import Essential
from sosw.labourer import Labourer
from sosw.managers.ecology import EcologyManager
from sosw.managers.meta_handler import MetaHandler
from sosw.managers.task import TaskManager
from sosw.orchestrator import Orchestrator
from sosw.scavenger import Scavenger
from sosw.scheduler import Scheduler
from sosw.worker import Worker
from sosw.worker_assistant import WorkerAssistant
from sosw.test.variables import TEST_ECOLOGY_CLIENT_CONFIG, TEST_ESSENTIAL_CONFIG, TEST_META_HANDLER_CONFIG, \
    TEST_ORCHESTRATOR_CONFIG, TEST_SCAVENGER_CONFIG, TEST_SCHEDULER_CONFIG, TEST_TASK_CLIENT_CONFIG, \
    TEST_WORKER_ASSISTANT_CONFIG


class Deprecations_UnitTestCase(unittest.TestCase):

    def setUp(self):
        reset_warned_entities()

        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()
        self.get_config_patch.return_value = {}


    def tearDown(self):
        self.patcher.stop()
        reset_warned_entities()

        # global_vars.processor is a property that refers to another global. So we have to reset it explicitly.
        global global_vars
        global_vars = LambdaGlobals()
        global_vars.processor = None

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass


    @staticmethod
    def catch_deprecations(factory):
        """
        Call the `factory` and return its result together with the recorded DeprecationWarnings.
        """

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            result = factory()

        return result, [w for w in caught if issubclass(w.category, DeprecationWarning)]


    def assert_warned_deprecation(self, caught, entity_name):
        """
        Assert that exactly one recorded warning deprecates `entity_name` with the standard message.
        """

        messages = [str(w.message) for w in caught]
        matching = [m for m in messages if m.startswith(f"{entity_name} is deprecated since sosw 3.0.0")]

        self.assertEqual(1, len(matching),
                         f"Expected exactly one DeprecationWarning for {entity_name}, caught: {messages}")
        self.assertIn('will be removed in 4.0', matching[0])
        self.assertIn(MIGRATION_GUIDE_URL, matching[0])


    # The `warn_deprecated` helper itself.
    def test_warn_deprecated__warns_once_per_entity(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            warn_deprecated('SomeEntity')
            warn_deprecated('SomeEntity')
            warn_deprecated('AnotherEntity')

        messages = [str(w.message) for w in caught]
        self.assertEqual(2, len(messages), messages)
        self.assertTrue(messages[0].startswith('SomeEntity is deprecated since sosw 3.0.0'))
        self.assertTrue(messages[1].startswith('AnotherEntity is deprecated since sosw 3.0.0'))


    def test_warn_deprecated__includes_hint(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            warn_deprecated('SomeEntity', hint='Use something else instead.')

        self.assertIn('Use something else instead.', str(caught[0].message))


    def test_reset_warned_entities__allows_rewarning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            warn_deprecated('SomeEntity')
            reset_warned_entities()
            warn_deprecated('SomeEntity')

        self.assertEqual(2, len(caught))


    # Every deprecated entity warns on instantiation.
    def test_orchestrator__warns_on_init(self):
        with patch('boto3.client'):
            _, caught = self.catch_deprecations(
                    lambda: Orchestrator(custom_config=deepcopy(TEST_ORCHESTRATOR_CONFIG)))

        self.assert_warned_deprecation(caught, 'Orchestrator')


    def test_scavenger__warns_on_init(self):
        with patch('boto3.client'):
            _, caught = self.catch_deprecations(lambda: Scavenger(custom_config=deepcopy(TEST_SCAVENGER_CONFIG)))

        self.assert_warned_deprecation(caught, 'Scavenger')


    def test_scheduler__warns_on_init(self):
        lambda_context = types.SimpleNamespace()
        lambda_context.aws_request_id = 'AWS_REQ_ID'
        lambda_context.invoked_function_arn = 'arn:aws:lambda:us-west-2:000000000000:function:some_function'
        lambda_context.get_remaining_time_in_millis = MagicMock(return_value=300000)
        global_vars.lambda_context = lambda_context

        with patch('boto3.client'):
            _, caught = self.catch_deprecations(lambda: Scheduler(custom_config=deepcopy(TEST_SCHEDULER_CONFIG)))

        self.assert_warned_deprecation(caught, 'Scheduler')


    def test_worker__warns_on_init(self):
        with patch('boto3.client'):
            _, caught = self.catch_deprecations(lambda: Worker())

        self.assert_warned_deprecation(caught, 'Worker')


    def test_worker_assistant__warns_on_init(self):
        with patch('boto3.client'):
            _, caught = self.catch_deprecations(
                    lambda: WorkerAssistant(custom_config=deepcopy(TEST_WORKER_ASSISTANT_CONFIG)))

        self.assert_warned_deprecation(caught, 'WorkerAssistant')


    def test_labourer__warns_on_init(self):
        _, caught = self.catch_deprecations(lambda: Labourer(id=42, arn='arn::aws::lambda'))

        self.assert_warned_deprecation(caught, 'Labourer')


    def test_essential__warns_on_init(self):
        with patch('boto3.client'):
            _, caught = self.catch_deprecations(lambda: Essential(custom_config=deepcopy(TEST_ESSENTIAL_CONFIG)))

        self.assert_warned_deprecation(caught, 'Essential')


    def test_task_manager__warns_on_init(self):
        with patch('boto3.client'):
            _, caught = self.catch_deprecations(lambda: TaskManager(custom_config=deepcopy(TEST_TASK_CLIENT_CONFIG)))

        self.assert_warned_deprecation(caught, 'TaskManager')


    def test_ecology_manager__warns_on_init(self):
        with patch('boto3.client'):
            _, caught = self.catch_deprecations(
                    lambda: EcologyManager(custom_config=deepcopy(TEST_ECOLOGY_CLIENT_CONFIG)))

        self.assert_warned_deprecation(caught, 'EcologyManager')


    def test_meta_handler__warns_on_init(self):
        with patch('boto3.client'):
            _, caught = self.catch_deprecations(lambda: MetaHandler(custom_config=deepcopy(TEST_META_HANDLER_CONFIG)))

        self.assert_warned_deprecation(caught, 'MetaHandler')


    def test_second_instantiation__does_not_rewarn(self):
        _, first = self.catch_deprecations(lambda: Labourer(id=42, arn='arn::aws::lambda'))
        self.assert_warned_deprecation(first, 'Labourer')

        _, second = self.catch_deprecations(lambda: Labourer(id=42, arn='arn::aws::lambda'))
        self.assertEqual([], [str(w.message) for w in second], "Once-guard failed: second instantiation re-warned")


    # The lazy package façade.
    def test_import_sosw__lazy_and_warning_free(self):
        """
        ``import sosw`` must emit no warnings and must import neither ``boto3`` nor the orchestration
        modules. Verified in a subprocess to get a clean interpreter without ``sosw`` pre-imported.
        """

        code = ("import sys, warnings; warnings.simplefilter('error'); import sosw; "
                "assert 'sosw.orchestrator' not in sys.modules, 'sosw.orchestrator imported eagerly'; "
                "assert 'boto3' not in sys.modules, 'boto3 imported eagerly'")

        result = subprocess.run([sys.executable, '-c', code], cwd=Path(__file__).resolve().parents[3],
                                capture_output=True, text=True, timeout=120)

        self.assertEqual(0, result.returncode, f"Subprocess failed: {result.stderr}")


    def test_facade__from_sosw_import_processor(self):
        from sosw import Processor as facade_processor

        self.assertIs(Processor, facade_processor)


    def test_facade__resolves_names_without_warnings(self):
        for name, expected in [('Processor', Processor), ('Orchestrator', Orchestrator), ('Worker', Worker),
                               ('Essential', Essential), ('Labourer', Labourer)]:
            with self.subTest(name=name):
                resolved, caught = self.catch_deprecations(lambda n=name: getattr(sosw, n))

                self.assertIs(expected, resolved)
                self.assertEqual([], [str(w.message) for w in caught],
                                 f"Resolving sosw.{name} must not warn before instantiation")


    def test_facade__unknown_attribute_raises(self):
        self.assertRaises(AttributeError, getattr, sosw, 'NoSuchAttribute')


    def test_facade__dir_contains_public_names(self):
        names = dir(sosw)

        for name in ['Processor', 'LambdaGlobals', 'get_lambda_handler', 'Essential', 'Labourer', 'Orchestrator',
                     'Scavenger', 'Scheduler', 'Worker', 'WorkerAssistant', '__version__']:
            self.assertIn(name, names)


    def test_version__is_string(self):
        self.assertIsInstance(sosw.__version__, str)
        self.assertTrue(len(sosw.__version__) > 0)


if __name__ == '__main__':
    unittest.main()
