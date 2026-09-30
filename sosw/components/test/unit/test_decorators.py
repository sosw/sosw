import logging
import unittest
import os

from unittest.mock import MagicMock, call, patch

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.components.decorators import logging_wrapper, retry


class decorators_UnitTestCase(unittest.TestCase):

    def setUp(self):
        self.patcher_sleep = patch('sosw.components.decorators.time.sleep')
        self.mock_sleep = self.patcher_sleep.start()
        self.addCleanup(self.patcher_sleep.stop)


    def test_logging_wrapper_logs_arguments_and_returns_result(self):
        """
        The docstring example of logging_wrapper: named arguments, extra positionals and keyword
        arguments are all logged, and the return value of the wrapped function is passed through.
        """

        @logging_wrapper(logging.INFO)
        def foo(a, b, c=4, *arglist, **keywords):
            return a, b, c, arglist, keywords

        with self.assertLogs(level=logging.INFO) as cm:
            result = foo(1, 2, 3, 4, 5, t=6, z=7)

        self.assertEqual(result, (1, 2, 3, (4, 5), {'t': 6, 'z': 7}))
        self.assertEqual(len(cm.records), 1)

        message = cm.records[0].getMessage()
        self.assertIn('foo', message)
        self.assertTrue(message.endswith('with a=1, b=2, c=3, 4, 5, t=6, z=7'))


    def test_logging_wrapper_default_level_is_info(self):
        """
        Without an explicit level the wrapper should log with logging.INFO.
        """

        @logging_wrapper()
        def greet(name):
            return f'Hello {name}'

        with self.assertLogs(level=logging.INFO) as cm:
            result = greet(name='John')

        self.assertEqual(result, 'Hello John')
        self.assertEqual(cm.records[0].levelno, logging.INFO)
        self.assertIn('name=John', cm.records[0].getMessage())


    def test_logging_wrapper_custom_level_and_no_arguments(self):
        """
        A custom level is respected and a call without arguments does not append 'with'.
        """

        @logging_wrapper(logging.DEBUG)
        def ping():
            return 'pong'

        with self.assertLogs(level=logging.DEBUG) as cm:
            result = ping()

        self.assertEqual(result, 'pong')
        self.assertEqual(cm.records[0].levelno, logging.DEBUG)

        message = cm.records[0].getMessage()
        self.assertTrue(message.endswith('ping'))
        self.assertNotIn(' with', message)


    def test_logging_wrapper_skips_self_argument_of_methods(self):
        """
        When decorating methods, the `self` argument must not be logged.
        """

        class Greeter:

            @logging_wrapper(logging.INFO)
            def greet(self, name):
                return f'Hello {name}'

        with self.assertLogs(level=logging.INFO) as cm:
            result = Greeter().greet('Bob')

        self.assertEqual(result, 'Hello Bob')

        message = cm.records[0].getMessage()
        self.assertIn('Greeter.greet', message)
        self.assertIn('name=Bob', message)
        self.assertNotIn('self=', message)


    def test_logging_wrapper_falls_back_when_introspection_fails(self):
        """
        Builtins have no `__code__`, so the wrapper logs the fallback message and still calls the function.
        """

        wrapped = logging_wrapper(logging.INFO)(len)

        with self.assertLogs(level=logging.INFO) as cm:
            result = wrapped('abc')

        self.assertEqual(result, 3)
        self.assertEqual(cm.records[0].getMessage(), "Running len with args=('abc',), kwargs={}")


    def test_logging_wrapper_preserves_name_and_docstring(self):
        """
        GitHub issue #14: the wrapper must use functools.wraps to keep the metadata of decorated methods.
        """

        @logging_wrapper()
        def documented(x):
            """Very important docstring."""
            return x

        self.assertEqual(documented.__name__, 'documented')
        self.assertEqual(documented.__doc__, 'Very important docstring.')
        self.assertTrue(hasattr(documented, '__wrapped__'))


    def test_retry_returns_result_on_first_success(self):
        func = MagicMock(return_value='ok')
        decorated = retry(ValueError, tries=4, delay=3)(func)

        result = decorated(1, key='value')

        self.assertEqual(result, 'ok')
        func.assert_called_once_with(1, key='value')
        self.mock_sleep.assert_not_called()


    def test_retry_retries_with_exponential_backoff_until_success(self):
        func = MagicMock(side_effect=[ValueError('first'), ValueError('second'), 'ok'])
        decorated = retry(ValueError, tries=4, delay=1, backoff=2)(func)

        with self.assertLogs(level=logging.WARNING) as cm:
            result = decorated('a', flag=True)

        self.assertEqual(result, 'ok')
        self.assertEqual(func.call_count, 3)
        func.assert_called_with('a', flag=True)
        self.assertEqual(self.mock_sleep.call_args_list, [call(1), call(2)])
        self.assertIn('first, Retrying in 1 seconds...', cm.output[0])
        self.assertIn('second, Retrying in 2 seconds...', cm.output[1])


    def test_retry_raises_after_exhausting_tries(self):
        func = MagicMock(side_effect=ValueError('persistent failure'))
        decorated = retry(ValueError, tries=3, delay=1, backoff=2)(func)

        with self.assertLogs(level=logging.WARNING):
            with self.assertRaises(ValueError) as context:
                decorated()

        self.assertEqual(str(context.exception), 'persistent failure')
        self.assertEqual(func.call_count, 3)
        self.assertEqual(self.mock_sleep.call_args_list, [call(1), call(2)])


    def test_retry_does_not_catch_unlisted_exceptions(self):
        func = MagicMock(side_effect=KeyError('nope'))
        decorated = retry(ValueError, tries=4, delay=1)(func)

        with self.assertRaises(KeyError):
            decorated()

        func.assert_called_once()
        self.mock_sleep.assert_not_called()


    def test_retry_supports_tuple_of_exceptions(self):
        """
        Both exception types from the tuple are retried, and backoff=1 keeps the delay constant.
        """

        func = MagicMock(side_effect=[ValueError('v'), KeyError('k'), 'done'])
        decorated = retry((ValueError, KeyError), tries=3, delay=1, backoff=1)(func)

        with self.assertLogs(level=logging.WARNING):
            result = decorated()

        self.assertEqual(result, 'done')
        self.assertEqual(func.call_count, 3)
        self.assertEqual(self.mock_sleep.call_args_list, [call(1), call(1)])


    def test_retry_single_try_calls_function_once(self):
        func = MagicMock(side_effect=ValueError('instant failure'))
        decorated = retry(ValueError, tries=1, delay=0)(func)

        with self.assertRaises(ValueError):
            decorated()

        func.assert_called_once()
        self.mock_sleep.assert_not_called()

        func_ok = MagicMock(return_value='ok')
        decorated_ok = retry(ValueError, tries=1, delay=0)(func_ok)

        self.assertEqual(decorated_ok(), 'ok')
        func_ok.assert_called_once()


    def test_retry_validates_decorator_arguments(self):
        with self.assertRaises(AssertionError) as context:
            retry(ValueError, tries=0)
        self.assertEqual(str(context.exception), "Tries must be 1 or greater")

        with self.assertRaises(AssertionError) as context:
            retry(ValueError, delay=-1)
        self.assertEqual(str(context.exception), "Delay must be greater than 0")

        with self.assertRaises(AssertionError) as context:
            retry(ValueError, backoff=0.5)
        self.assertEqual(str(context.exception), "Backoff must be greater than 1")


    def test_retry_falls_back_to_print_when_logging_unavailable(self):
        """
        If the module-level `logging` is falsy, the retry message is printed instead.
        Also exercises the default exception_to_check=Exception.
        """

        func = MagicMock(side_effect=[ValueError('boom'), 'ok'])
        decorated = retry(tries=2, delay=1)(func)

        with patch('sosw.components.decorators.logging', None), patch('builtins.print') as mock_print:
            result = decorated()

        self.assertEqual(result, 'ok')
        self.assertEqual(func.call_count, 2)
        self.assertEqual(self.mock_sleep.call_args_list, [call(1)])
        mock_print.assert_called_once_with('boom, Retrying in 1 seconds...')


    def test_retry_preserves_name_and_docstring(self):
        @retry(ValueError, tries=2, delay=1)
        def documented_function(x):
            """Docstring survives decoration."""
            return x

        self.assertEqual(documented_function.__name__, 'documented_function')
        self.assertEqual(documented_function.__doc__, 'Docstring survives decoration.')
        self.assertEqual(documented_function(42), 42)


if __name__ == '__main__':
    unittest.main()
