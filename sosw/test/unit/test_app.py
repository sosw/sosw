import datetime

import boto3
import os
import unittest

from unittest.mock import MagicMock, patch

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.app import Processor, LambdaGlobals, get_lambda_handler, logger
from sosw.components.sns import SnsManager
from sosw.components.siblings import SiblingsManager


class app_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = {'test': True}


    class Child(Processor):
        def __call__(self, event):
            super().__call__(event)
            return event.get('k')


    def setUp(self):
        pass

    def tearDown(self):
        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass

        # global_vars.processor is a property that refers to another global. So we have to reset it explicitly.
        # And at the same time we don't want to reset it during reinitialization in the working environment
        global _processor, global_vars
        global_vars = LambdaGlobals()
        global_vars.processor = None


    @patch("boto3.client")
    def test_app_init(self, mock_boto_client):
        Processor(custom_config=self.TEST_CONFIG)
        self.assertTrue(True)


    @patch("boto3.client")
    def test_app__pre_call__reset_stats(self, _):
        processor = Processor(custom_config=self.TEST_CONFIG)
        processor.__call__(event={'k': 'success'})
        self.assertEqual(processor.stats['processor_calls'], 1)
        processor.__pre_call__()
        self.assertNotIn('processor_calls', processor.stats)
        self.assertEqual(processor.stats['total_processor_calls'], 1)


    @patch("boto3.client")
    def test_app_init__with_some_clients(self, mock_boto_client):
        custom_config = {
            'init_clients': ['Sns', 'Siblings'],
            'siblings_config': {
                "test": True
            }
        }

        processor = Processor(custom_config=custom_config)
        self.assertIsInstance(getattr(processor, 'sns_client'), SnsManager,
                              "SnsManager was not initialized. Probably boto3 sns instead of it.")
        self.assertIsNotNone(getattr(processor, 'siblings_client'))


    @patch("boto3.client")
    def test_app_init__boto_and_components_custom_clients(self, mock_boto_client):
        custom_config = {
            'init_clients': ['dynamodb', 'Siblings'],
            'siblings_config': {
                "test": True
            }
        }

        processor = Processor(custom_config=custom_config)
        self.assertIsInstance(getattr(processor, 'siblings_client'), SiblingsManager)

        # Clients of boto3 will not be exactly of same type (something dynamic in boto3), so we can't compare classes.
        # Let us assume that checking the class_name is enough for this test.
        self.assertEqual(str(type(getattr(processor, 'dynamodb_client'))), str(type(boto3.client('dynamodb'))))


    @patch("boto3.client")
    def test_app_init__with_some_invalid_client(self, mock_boto_client):
        custom_config = {
            'init_clients': ['NotExists']
        }
        Processor(custom_config=custom_config)
        mock_boto_client.assert_called_with('not_exists')


    @patch("boto3.client")
    def test_register_clients__raises_when_boto3_fallback_fails(self, mock_boto_client):
        """
        If a client is neither importable from components/managers nor a valid boto3 service,
        register_clients must fail fast and loud.
        """

        mock_boto_client.side_effect = Exception("Unknown service")

        with self.assertRaises(RuntimeError) as exc:
            Processor(custom_config={'init_clients': ['NotExists']})

        self.assertIn("Failed to import for service not_exists", str(exc.exception))


    @patch("boto3.client")
    def test_register_clients__raises_when_module_has_no_client_class(self, mock_boto_client):
        """
        The module `sosw.components.helpers` imports fine, but has neither HelpersManager nor HelpersClient.
        """

        with self.assertRaises(RuntimeError) as exc:
            Processor(custom_config={'init_clients': ['Helpers']})

        self.assertIn("Failed to import Helpers", str(exc.exception))
        self.assertIn("Manager", str(exc.exception))
        self.assertIn("Client", str(exc.exception))


    @patch("sosw.app.get_config")
    def test_app_calls_get_config(self, mock_ssm):

        mock_ssm.return_value = {'mock': 'called'}
        os.environ['AWS_LAMBDA_FUNCTION_NAME'] = 'test_func'

        Processor(custom_config=self.TEST_CONFIG)
        mock_ssm.assert_called_once_with('test_func_config')


    @patch("sosw.app.get_config")
    def test_init__test_flag_precedence(self, mock_ssm):
        """
        An explicitly passed `test` flag must always win. Otherwise the flag is derived from STAGE.
        """

        mock_ssm.return_value = {}

        matrix = [
            # (explicit_flag, stage, expected)
            (True, 'test', True),
            (False, 'test', False),
            (None, 'test', True),
            (True, 'prod', True),
            (False, 'prod', False),
            (None, 'prod', False),
        ]

        for explicit_flag, stage, expected in matrix:
            with self.subTest(explicit_flag=explicit_flag, stage=stage):
                kwargs = {} if explicit_flag is None else {'test': explicit_flag}
                with patch.dict(os.environ, {'STAGE': stage}):
                    processor = Processor(custom_config=self.TEST_CONFIG, **kwargs)
                self.assertEqual(processor.test, expected)


    @patch("sosw.app.get_config")
    def test_init_config__disable_ddb_config__from_custom_config(self, mock_ssm):

        os.environ['AWS_LAMBDA_FUNCTION_NAME'] = 'test_func'

        processor = Processor(custom_config={'disable_ddb_config': True, 'foo': 'bar'})

        mock_ssm.assert_not_called()
        self.assertEqual(processor.config['foo'], 'bar', "custom_config must still be applied")


    @patch("sosw.app.get_config")
    def test_init_config__disable_ddb_config__from_class_attribute(self, mock_ssm):

        class NoDdbConfigProcessor(Processor):
            DISABLE_DDB_CONFIG = True

        os.environ['AWS_LAMBDA_FUNCTION_NAME'] = 'test_func'

        processor = NoDdbConfigProcessor(custom_config=self.TEST_CONFIG)

        mock_ssm.assert_not_called()
        self.assertEqual(processor.config['test'], True, "custom_config must still be applied")


    @patch("sosw.app.get_config")
    def test_init_config__disable_ddb_config__from_default_config(self, mock_ssm):

        class DefaultsProcessor(Processor):
            DEFAULT_CONFIG = {'disable_ddb_config': True, 'some_default': 42}

        os.environ['AWS_LAMBDA_FUNCTION_NAME'] = 'test_func'

        processor = DefaultsProcessor()

        mock_ssm.assert_not_called()
        self.assertEqual(processor.config['some_default'], 42, "DEFAULT_CONFIG must still be applied")


    # @unittest.skip("https://github.com/bimpression/sosw/issues/40")
    # def test__account(self):
    #     raise NotImplementedError
    #
    #
    # @unittest.skip("https://github.com/bimpression/sosw/issues/40")
    # def test__region(self):
    #     raise NotImplementedError


    def test_lambda_handler(self):

        mock_context = MagicMock()
        mock_context.invoked_function_arn = 'arn:aws:lambda:us-east-1:123456789012:function:example:42'

        global_vars = LambdaGlobals()
        self.assertIsNone(global_vars.processor)
        self.assertIsNone(global_vars.lambda_context)

        lambda_handler = get_lambda_handler(self.Child, global_vars, self.TEST_CONFIG)
        self.assertIsNotNone(lambda_handler)

        for i in range(3):
            result = lambda_handler(event={'k': 'success'}, context=mock_context)
            self.assertEqual(type(global_vars.processor), self.Child)
            self.assertEqual(global_vars.lambda_context, mock_context)
            self.assertEqual(result, 'success')
            self.assertEqual(global_vars.processor.stats['total_processor_calls'], i + 1)
            self.assertEqual(global_vars.processor.stats['total_calls_register_clients'], 1)


    def test_lambda_handler__test_flag_precedence(self):
        """
        The `test` key of the event must always win. Otherwise the flag is derived from STAGE.
        """

        matrix = [
            # (event_test_key, stage, expected)
            (True, 'test', True),
            (False, 'test', False),
            (None, 'test', True),
            (True, 'prod', True),
            (False, 'prod', False),
            (None, 'prod', False),
        ]

        for event_flag, stage, expected in matrix:
            with self.subTest(event_flag=event_flag, stage=stage):
                global_vars = LambdaGlobals()
                global_vars.processor = None

                processor_class = MagicMock()
                lambda_handler = get_lambda_handler(processor_class, global_vars, self.TEST_CONFIG)

                event = {'k': 'v'} if event_flag is None else {'k': 'v', 'test': event_flag}
                with patch.dict(os.environ, {'STAGE': stage}):
                    lambda_handler(event=event, context=MagicMock())

                processor_class.assert_called_once_with(custom_config=self.TEST_CONFIG, test=expected)


    def test_lambda_handler__non_dict_event(self):
        """
        The handler must accept non-dict payloads: the `test` flag then derives from STAGE only.
        """

        global_vars = LambdaGlobals()
        global_vars.processor = None

        processor_class = MagicMock()
        lambda_handler = get_lambda_handler(processor_class, global_vars, self.TEST_CONFIG)

        with patch.dict(os.environ, {'STAGE': 'prod'}):
            lambda_handler(event=['not', 'a', 'dict'], context=MagicMock())

        processor_class.assert_called_once_with(custom_config=self.TEST_CONFIG, test=False)


    def test_lambda_handler__caches_processor_across_invocations(self):
        """
        Warm start contract: the Processor is constructed on the cold start only and then reused.
        """

        global_vars = LambdaGlobals()
        global_vars.processor = None

        processor_class = MagicMock()
        lambda_handler = get_lambda_handler(processor_class, global_vars, self.TEST_CONFIG)

        lambda_handler(event={'k': 1}, context=MagicMock())
        first_processor = global_vars.processor
        lambda_handler(event={'k': 2}, context=MagicMock())

        processor_class.assert_called_once()
        self.assertIs(global_vars.processor, first_processor)


    def test_lambda_handler__resets_stats_once_per_invocation(self):
        """
        `reset_stats()` must be called exactly once per invocation, recursively.
        """

        global_vars = LambdaGlobals()
        global_vars.processor = None

        processor_class = MagicMock()
        lambda_handler = get_lambda_handler(processor_class, global_vars, self.TEST_CONFIG)

        lambda_handler(event={'k': 1}, context=MagicMock())
        processor_instance = processor_class.return_value
        processor_instance.reset_stats.assert_called_once_with(recursive=True)

        lambda_handler(event={'k': 2}, context=MagicMock())
        self.assertEqual(processor_instance.reset_stats.call_count, 2)


    @patch.object(logger, 'error')
    def test_get_lambda_handler__missing_global_vars(self, mock_logger_error):
        """
        Missing global_vars is reported, but the handler must still work on the module-level globals.
        """

        processor_class = MagicMock()
        lambda_handler = get_lambda_handler(processor_class)

        mock_logger_error.assert_called_once()

        lambda_handler(event={'k': 1}, context=MagicMock())
        processor_class.assert_called_once()


    def test_property_account__initialized_from_context(self):
        mock_context = MagicMock()
        mock_context.invoked_function_arn = 'arn:aws:lambda:us-east-1:123456789000:function:example:42'

        self.assertIsNone(global_vars.lambda_context)

        lambda_handler = get_lambda_handler(self.Child, global_vars, self.TEST_CONFIG)
        lambda_handler(event={'k': 'success'}, context=mock_context)

        self.assertEqual('123456789000', global_vars.processor._account)


    @patch("boto3.client")
    def test_property_account__initialized_from_sts(self, boto_client_mock):

        get_caller_identity_mock = MagicMock()
        get_caller_identity_mock.get.return_value='001234567890'

        client_mock = MagicMock()
        client_mock.get_caller_identity.return_value = get_caller_identity_mock

        boto_client_mock.return_value = client_mock

        p = Processor()
        self.assertEqual('001234567890', p._account)
        get_caller_identity_mock.get.assert_called_once_with('Account')


    @patch("boto3.client")
    @patch.object(logger, 'setLevel')
    def test_lambda_handler__logger_level(self, logger_set_level, client_mock):
        global_vars = LambdaGlobals()
        lambda_handler = get_lambda_handler(self.Child, global_vars, self.TEST_CONFIG)
        event = {'k': 'm', 'logging_level': 20}
        lambda_handler(event=event, context=None)
        logger_set_level.assert_called_once_with(20)


    @patch("boto3.client")
    def test_die(self, mock_boto):

        p = Processor(custom_config=self.TEST_CONFIG)

        with self.assertRaises(SystemExit):
            p.die()


    @patch("boto3.client")
    def test_die__uncatchable_death(self, mock_boto):

        class Child(Processor):
            def catch_me(self):
                try:
                    self.die()
                except Exception:
                    pass

        p = Child(custom_config=self.TEST_CONFIG)

        with self.assertRaises(SystemExit):
            p.catch_me()


    @patch("boto3.client")
    def test_die__calls_sns(self, mock_boto):

        mock_boto_client = MagicMock()
        mock_boto.return_value = mock_boto_client

        p = Processor(custom_config=self.TEST_CONFIG)

        with self.assertRaises(SystemExit):
            p.die()

        mock_boto_client.publish.assert_called_once()
        args, kwargs = mock_boto_client.publish.call_args
        self.assertIn('SoswWorkerErrors', kwargs['TopicArn'])
        self.assertEqual(kwargs['Subject'], 'Some Function died')
        self.assertEqual(kwargs['Message'], 'Unknown Failure')


    @patch("boto3.client")
    def test_die__sns_failure_still_raises_system_exit(self, mock_boto):
        """
        Even if publishing the death notice to SNS fails, die() must log that and still exit.
        """

        p = Processor(custom_config=self.TEST_CONFIG)
        mock_boto.side_effect = Exception("No SNS access")

        with patch.object(logger, 'exception') as mock_logger_exception:
            with self.assertRaises(SystemExit) as exc:
                p.die("Some failure")

        self.assertEqual(exc.exception.code, 1)
        mock_logger_exception.assert_any_call("Failed to send SNS message to Alarms.")


    @patch("boto3.client")
    def test_get_stats__recursive_merges_stats_of_clients(self, _):
        p = Processor(custom_config=self.TEST_CONFIG)
        p.stats['own_calls'] = 3
        p.foo_client = MagicMock()
        p.foo_client.get_stats.return_value = {'foo_stat': 42}
        p.bar_client = object()  # A client without get_stats() implemented. Must be silently skipped.

        stats = p.get_stats()

        self.assertEqual(stats['foo_stat'], 42)
        self.assertEqual(stats['own_calls'], 3)
        p.foo_client.get_stats.assert_called_once()


    @patch("boto3.client")
    def test_get_stats__not_recursive_skips_clients(self, _):
        p = Processor(custom_config=self.TEST_CONFIG)
        p.foo_client = MagicMock()

        stats = p.get_stats(recursive=False)

        self.assertNotIn('foo_stat', stats)
        p.foo_client.get_stats.assert_not_called()


    @patch("boto3.client")
    def test_reset_stats__skips_non_numeric_values(self, _):
        p = Processor(custom_config=self.TEST_CONFIG)
        p.stats['numeric'] = 5
        p.stats['labourer_name'] = 'some_function'

        p.reset_stats()

        self.assertEqual(p.stats['total_numeric'], 5)
        self.assertNotIn('labourer_name', p.stats)
        self.assertNotIn('total_labourer_name', p.stats)


    @patch("boto3.client")
    def test_reset_stats__recursive_resets_clients(self, _):
        p = Processor(custom_config=self.TEST_CONFIG)
        p.foo_client = MagicMock()
        p.bar_client = object()  # A client without reset_stats() implemented. Must be silently skipped.

        p.reset_stats()

        p.foo_client.reset_stats.assert_called_once()


    @patch("boto3.client")
    def test_reset_stats__not_recursive_skips_clients(self, _):
        p = Processor(custom_config=self.TEST_CONFIG)
        p.foo_client = MagicMock()

        p.reset_stats(recursive=False)

        p.foo_client.reset_stats.assert_not_called()


    @patch("boto3.client")
    def test_exit__closes_connections(self, _):
        p = Processor(custom_config=self.TEST_CONFIG)
        p.sql = MagicMock()
        p.conn = MagicMock()

        p.__exit__(None, None, None)

        p.sql.sqldb.session.remove.assert_called_once()
        p.conn.close.assert_called_once()


    @patch("boto3.client")
    def test_exit__survives_missing_connections(self, _):
        p = Processor(custom_config=self.TEST_CONFIG)

        # Must not raise for a Processor without `sql` or `conn` attributes.
        p.__exit__(None, None, None)


    @patch("boto3.client")
    @patch("sosw.app.DynamoDbClient")
    def test_get_ddbc(self, mock_dynamodb_client, _):
        """
         Tests the `get_ddbc` method of Processor class with a valid prefix and configuration.

         This test verifies that:
             * `mock_dynamodb_client` is called once with the correct arguments.
             * The returned client instance is an instance of `DynamoDbClient`.
         """

        prefix = 'example'
        config = {
            'example_dynamo_db_config': {'table_name': 'example_table'},
        }

        # mock_dynamodb_client.return_value = MagicMock()

        processor = Processor(custom_config=config)
        client_instance = processor.get_ddbc(prefix)

        mock_dynamodb_client.assert_called_once_with(config['example_dynamo_db_config'])
        self.assertIsInstance(client_instance, MagicMock)


    def test_get_ddbc_invalid_prefix(self):
        """
           Tests the `get_ddbc` method of Processor class when an invalid prefix is provided.

           This test verifies that:
               * A `ValueError` is raised when an invalid prefix is provided.
               * The error message contains the expected message indicating the supported prefixes.
           """

        prefix = 'invalid'
        config = {
            'example_dynamo_db_config': {'table_name': 'example_table'},
        }

        processor = Processor(custom_config=config)

        with self.assertRaises(ValueError) as context:
            processor.get_ddbc(prefix)

            self.assertEqual(str(context.exception), "get_ddbc() method supports only prefixes: ['example']")

    def test_c(self):
        p = Processor(custom_config={'a': {'b': {'c': 42}}})

        self.assertEqual(p._c('a.b.c'), 42)
        self.assertEqual(p._c('a.b.z'), None)
        self.assertEqual(p._c('z'), None)

    def test_c_default(self):
        p = Processor(custom_config={'a': {'b': {'c': 42}}})

        self.assertEqual(p._c('a.b.z', 'foo'), 'foo')
        self.assertEqual(p._c('z', 42.2), 42.2)

        dt = datetime.datetime.now()
        self.assertEqual(p._c('z', dt), dt)
