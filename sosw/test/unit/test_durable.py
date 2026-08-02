import importlib
import os
import subprocess
import sys
import types
import unittest

from unittest.mock import MagicMock, patch

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

import sosw.app
import sosw.durable

from sosw.app import LambdaGlobals, Processor


SDK_NAME = 'aws_durable_execution_sdk_python'
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))


def make_fake_sdk():
    """
    Build a fake `aws_durable_execution_sdk_python` module pair exposing the decorator surface
    used by `sosw.durable`: `durable_execution` and `durable_step` in the root module and
    `Duration` in the `config` submodule.
    """

    root = types.ModuleType(SDK_NAME)
    config = types.ModuleType(f"{SDK_NAME}.config")

    def fake_durable_execution(func, **kwargs):
        def wrapper(event, context):
            return func(event, context)

        wrapper.durable_wrapped = func
        wrapper.durable_kwargs = kwargs
        return wrapper

    root.durable_execution = MagicMock(side_effect=fake_durable_execution)
    root.durable_step = MagicMock(name='durable_step')
    root.config = config
    config.Duration = MagicMock(name='Duration')

    return root, config


class durable_UnitTestCase(unittest.TestCase):
    """
    Tests of `sosw.durable` with the durable SDK NOT installed. This is the natural state of the
    unit test environment: the SDK is an optional extra of sosw and must never be required here.
    """


    def setUp(self):
        # Block the SDK import even if somebody installed the real SDK into the test environment.
        # `None` in sys.modules forces an ImportError for the module and its submodules.
        for name in [SDK_NAME, f"{SDK_NAME}.config"]:
            sys.modules.pop(name, None)
            sys.modules[name] = None

        self.durable = importlib.reload(sosw.durable)


    def tearDown(self):
        for name in [SDK_NAME, f"{SDK_NAME}.config"]:
            sys.modules.pop(name, None)
        importlib.reload(sosw.durable)

        # global_vars.processor and lambda_context are properties over module-level globals. Reset them.
        global_vars = LambdaGlobals()
        global_vars.processor = None


    def test_module_imports_without_sdk(self):
        self.assertFalse(self.durable.DURABLE_SDK_AVAILABLE)
        self.assertIsNone(self.durable.durable_execution)
        self.assertIsNone(self.durable.durable_step)
        self.assertIsNone(self.durable.Duration)


    def test_get_durable_lambda_handler__raises_helpful_import_error(self):
        with self.assertRaises(ImportError) as cm:
            self.durable.get_durable_lambda_handler(MagicMock(), LambdaGlobals(), {'test': True})

        self.assertIn('pip install sosw[durable]', str(cm.exception))


    def test_import_sosw_app__does_not_import_sdk(self):
        """
        `import sosw.app` must never pull the durable SDK: zero overhead for non-durable functions.
        Verified in a pristine interpreter so that this test does not depend on the state of ours.
        """

        code = ("import sys\n"
                "import sosw.app\n"
                f"raise SystemExit(1 if '{SDK_NAME}' in sys.modules else 0)\n")

        completed = subprocess.run([sys.executable, '-c', code], cwd=REPO_ROOT,
                                   capture_output=True, text=True, timeout=120)

        self.assertEqual(completed.returncode, 0,
                         f"`import sosw.app` must not import the durable SDK. Stderr: {completed.stderr}")


    @patch("sosw.durable.time")
    def test_durable_wait__falls_back_to_sleep__no_context(self, mock_time):
        global_vars = LambdaGlobals()

        self.durable.durable_wait(3, global_vars=global_vars)

        mock_time.sleep.assert_called_once_with(3)


    @patch("sosw.durable.time")
    def test_durable_wait__falls_back_to_sleep__context_without_wait(self, mock_time):
        global_vars = LambdaGlobals()
        global_vars.lambda_context = object()   # A regular LambdaContext has no `wait` operation.

        self.durable.durable_wait(4, global_vars=global_vars)

        mock_time.sleep.assert_called_once_with(4)


    @patch("sosw.durable.time")
    def test_durable_wait__falls_back_to_sleep__waitable_context_but_no_sdk(self, mock_time):
        """Without the SDK there is no Duration to construct, so we must sleep even if ctx can wait."""

        global_vars = LambdaGlobals()
        global_vars.lambda_context = MagicMock()

        self.durable.durable_wait(5, global_vars=global_vars)

        mock_time.sleep.assert_called_once_with(5)
        global_vars.lambda_context.wait.assert_not_called()


    @patch("sosw.durable.time")
    def test_durable_wait__defaults_to_module_global_vars(self, mock_time):
        sosw.app.global_vars.lambda_context = None

        self.durable.durable_wait(1)

        mock_time.sleep.assert_called_once_with(1)


    def test_parse_durable_result__succeeded_with_json_string(self):
        payload = {'Status': 'SUCCEEDED', 'Result': '{"a": 1}'}
        self.assertEqual(self.durable.parse_durable_result(payload), {'a': 1})


    def test_parse_durable_result__succeeded_with_dict_result(self):
        payload = {'Status': 'SUCCEEDED', 'Result': {'a': 1}}
        self.assertEqual(self.durable.parse_durable_result(payload), {'a': 1})


    def test_parse_durable_result__succeeded_with_non_dict_json(self):
        payload = {'Status': 'SUCCEEDED', 'Result': '"hello"'}
        self.assertEqual(self.durable.parse_durable_result(payload), 'hello')


    def test_parse_durable_result__passes_through_non_envelopes(self):
        for payload in [{'foo': 'bar'}, {'Status': 'OK', 'Result': 1}, ['a', 'b'], 'raw', 42, None]:
            with self.subTest(payload=payload):
                self.assertEqual(self.durable.parse_durable_result(payload), payload)


    def test_parse_durable_result__failed(self):
        payload = {'Status': 'FAILED', 'Error': {'Message': 'boom'}}

        with self.assertRaises(RuntimeError) as cm:
            self.durable.parse_durable_result(payload)

        self.assertIn('boom', str(cm.exception))


    def test_parse_durable_result__failed_without_error_details(self):
        with self.assertRaises(RuntimeError) as cm:
            self.durable.parse_durable_result({'Status': 'FAILED'})

        self.assertIn('unknown', str(cm.exception))


    def test_parse_durable_result__pending(self):
        with self.assertRaises(RuntimeError):
            self.durable.parse_durable_result({'Status': 'PENDING'})


    def test_parse_durable_result__succeeded_with_empty_result(self):
        for payload in [{'Status': 'SUCCEEDED'}, {'Status': 'SUCCEEDED', 'Result': ''},
                        {'Status': 'SUCCEEDED', 'Result': '   '}]:
            with self.subTest(payload=payload):
                with self.assertRaises(RuntimeError):
                    self.durable.parse_durable_result(payload)


    def test_parse_durable_result__succeeded_with_invalid_json(self):
        with self.assertRaises(RuntimeError):
            self.durable.parse_durable_result({'Status': 'SUCCEEDED', 'Result': '{not json'})


    def test_parse_durable_result__succeeded_with_unexpected_result_type(self):
        with self.assertRaises(RuntimeError):
            self.durable.parse_durable_result({'Status': 'SUCCEEDED', 'Result': 42})


class durable_WithFakeSdk_UnitTestCase(unittest.TestCase):
    """
    Tests of `sosw.durable` with a FAKE durable SDK injected into sys.modules. Covers the code paths
    that require the SDK without ever installing it in the unit test environment.
    """

    TEST_CONFIG = {'test': True}


    class Child(Processor):
        def __call__(self, event):
            super().__call__(event)
            return event.get('k')


    def setUp(self):
        self.fake_root, self.fake_config = make_fake_sdk()
        sys.modules[SDK_NAME] = self.fake_root
        sys.modules[f"{SDK_NAME}.config"] = self.fake_config

        self.durable = importlib.reload(sosw.durable)


    def tearDown(self):
        sys.modules.pop(SDK_NAME, None)
        sys.modules.pop(f"{SDK_NAME}.config", None)
        importlib.reload(sosw.durable)

        # global_vars.processor and lambda_context are properties over module-level globals. Reset them.
        global_vars = LambdaGlobals()
        global_vars.processor = None


    def test_sdk_surface_is_reexported(self):
        self.assertTrue(self.durable.DURABLE_SDK_AVAILABLE)
        self.assertIs(self.durable.durable_execution, self.fake_root.durable_execution)
        self.assertIs(self.durable.durable_step, self.fake_root.durable_step)
        self.assertIs(self.durable.Duration, self.fake_config.Duration)


    def test_get_durable_lambda_handler__wraps_handler(self):
        global_vars = LambdaGlobals()
        global_vars.processor = None

        handler = self.durable.get_durable_lambda_handler(MagicMock(), global_vars, self.TEST_CONFIG)

        self.fake_root.durable_execution.assert_called_once()
        args, kwargs = self.fake_root.durable_execution.call_args
        self.assertTrue(callable(args[0]))
        self.assertEqual(kwargs, {})
        self.assertIs(handler.durable_wrapped, args[0], "The returned handler must be the decorated one")


    def test_get_durable_lambda_handler__passes_durable_kwargs(self):
        global_vars = LambdaGlobals()
        global_vars.processor = None

        handler = self.durable.get_durable_lambda_handler(MagicMock(), global_vars, None,
                                                          timeout=900, retries=2)

        args, kwargs = self.fake_root.durable_execution.call_args
        self.assertEqual(kwargs, {'timeout': 900, 'retries': 2})
        self.assertEqual(handler.durable_kwargs, {'timeout': 900, 'retries': 2})


    def test_durable_handler__executes_processor_lifecycle(self):
        """
        The wrapped handler must keep the exact non-durable Processor lifecycle:
        warm-start cache of the Processor, fresh lambda_context and stats reset per invocation.
        """

        global_vars = LambdaGlobals()
        global_vars.processor = None

        handler = self.durable.get_durable_lambda_handler(self.Child, global_vars, self.TEST_CONFIG)

        mock_context = MagicMock()
        mock_context.invoked_function_arn = 'arn:aws:lambda:us-east-1:123456789012:function:example:42'

        for i in range(2):
            result = handler(event={'k': 'success'}, context=mock_context)
            self.assertEqual(result, 'success')
            self.assertEqual(type(global_vars.processor), self.Child)
            self.assertEqual(global_vars.lambda_context, mock_context)
            self.assertEqual(global_vars.processor.stats['total_processor_calls'], i + 1)


    def test_durable_wait__uses_context_wait(self):
        global_vars = LambdaGlobals()
        ctx = MagicMock()
        global_vars.lambda_context = ctx

        self.durable.durable_wait(7, global_vars=global_vars)

        self.fake_config.Duration.assert_called_once_with(seconds=7)
        ctx.wait.assert_called_once_with(self.fake_config.Duration.return_value)


if __name__ == '__main__':
    unittest.main()
