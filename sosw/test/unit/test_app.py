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


    @patch("sosw.app.get_config")
    def test_app_calls_get_config(self, mock_ssm):

        mock_ssm.return_value = {'mock': 'called'}
        os.environ['AWS_LAMBDA_FUNCTION_NAME'] = 'test_func'

        Processor(custom_config=self.TEST_CONFIG)
        mock_ssm.assert_called_once_with('test_func_config')


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
