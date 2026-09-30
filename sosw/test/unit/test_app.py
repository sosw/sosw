import copy
import datetime
import json
import os
import unittest

import boto3

from decimal import Decimal
from unittest.mock import MagicMock, call, patch

os.environ['STAGE'] = 'test'
os.environ['autotest'] = 'True'

import sosw.app
from sosw.app import (LOG_REDACTED_VALUE, Processor, LambdaGlobals, _log_redacted, _redact_for_logging,
                      get_lambda_handler, logger)
from sosw.components.dynamo_db import DynamoDbClient
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
    def test_app_init__client_class_receives_config(self, mock_boto_client):
        """
        Classes with the `Client` suffix (e.g. DynamoDbClient) receive their config as `config`.
        """

        custom_config = {
            'init_clients':     ['DynamoDb'],
            'dynamo_db_config': {
                'row_mapper':      {'hash_col': 'S'},
                'required_fields': ['hash_col'],
                'table_name':      'autotest_dynamo_db',
            }
        }

        processor = Processor(custom_config=custom_config)

        self.assertIsInstance(getattr(processor, 'dynamo_db_client'), DynamoDbClient)
        self.assertEqual(processor.dynamo_db_client.config['table_name'], 'autotest_dynamo_db')


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


    @patch("boto3.client")
    @patch.object(logger, 'info')
    def test_lambda_handler__redacts_logged_event_and_result(self, mock_logger_info, _):
        """
        The handler logs redacted copies of the event and the result — headers, the raw query
        string, the JSON request body and the tokens issued in the response — but the Processor
        receives the original event object and the original result object is returned to the caller.
        """

        received = {}


        class RecordingChild(Processor):
            def __call__(self, event):
                super().__call__(event)
                received['event'] = event
                received['result'] = {
                    'statusCode': 200,
                    'headers':    {'Set-Cookie': 'test-set-cookie-value'},
                    'body':       json.dumps({'access_token': 'test-issued-token'}),
                }
                return received['result']


        global_vars = LambdaGlobals()
        lambda_handler = get_lambda_handler(RecordingChild, global_vars, self.TEST_CONFIG)

        mock_context = MagicMock()
        mock_context.invoked_function_arn = 'arn:aws:lambda:us-east-1:123456789012:function:example:42'

        body = json.dumps({'username': 'test-user', 'password': 'test-body-password'})
        event = {
            'headers':        {'Authorization': 'test-jwt-value', 'X-Origin-Verify': 'test-edge-secret-value'},
            'requestContext': {'authorizer': {'claims': {'sub': 'test-sub-claim'}}},
            'rawQueryString': 'access_token=test-raw-query-token&limit=10',
            'body':           body,
        }
        result = lambda_handler(event=event, context=mock_context)

        # The Processor got the exact same event object, with the secrets intact.
        self.assertIs(received['event'], event)
        self.assertEqual(event['headers']['Authorization'], 'test-jwt-value')
        self.assertEqual(event['headers']['X-Origin-Verify'], 'test-edge-secret-value')
        self.assertEqual(event['rawQueryString'], 'access_token=test-raw-query-token&limit=10')
        self.assertEqual(event['body'], body)

        # The handler returned the exact same result object, un-redacted.
        self.assertIs(result, received['result'])
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['headers'], {'Set-Cookie': 'test-set-cookie-value'})
        self.assertEqual(result['body'], json.dumps({'access_token': 'test-issued-token'}))

        # None of the secrets leaked into any logger.info call, but the redaction marker is there.
        logged = ' '.join(str(call) for call in mock_logger_info.call_args_list)
        for secret in ('test-jwt-value', 'test-edge-secret-value', 'test-raw-query-token', 'test-body-password',
                       'test-issued-token', 'test-set-cookie-value'):
            self.assertNotIn(secret, logged)
        self.assertIn(LOG_REDACTED_VALUE, logged)


    @patch("boto3.client")
    @patch.object(logger, 'info')
    def test_lambda_handler__deeply_nested_body_still_calls_processor(self, mock_logger_info, _):
        """
        A 2000-deep JSON body cannot be redacted recursively: the logged event carries the marker
        for it instead of the raw body, but the Processor is still called with the original event
        object and the original result object is returned to the caller.
        """

        received = {}


        class RecordingChild(Processor):
            def __call__(self, event):
                super().__call__(event)
                received['event'] = event
                received['result'] = {'statusCode': 200}
                return received['result']


        global_vars = LambdaGlobals()
        lambda_handler = get_lambda_handler(RecordingChild, global_vars, self.TEST_CONFIG)

        deep_body = '[' * 2000 + ']' * 2000
        mock_context = MagicMock()
        mock_context.invoked_function_arn = 'arn:aws:lambda:us-east-1:123456789012:function:example:42'
        event = {'body': deep_body}
        result = lambda_handler(event=event, context=mock_context)

        self.assertIs(received['event'], event)
        self.assertEqual(event['body'], deep_body)
        self.assertIs(result, received['result'])

        logged_events = [c.args[0] for c in mock_logger_info.call_args_list
                         if isinstance(c.args[0], dict) and 'body' in c.args[0]]
        self.assertEqual(len(logged_events), 1)
        self.assertEqual(logged_events[0]['body'], LOG_REDACTED_VALUE)
        self.assertNotIn(deep_body, ' '.join(str(c) for c in mock_logger_info.call_args_list))


    @patch.object(logger, 'info')
    def test_lambda_handler__too_deep_event_logs_marker(self, mock_logger_info):
        """
        The handler-level redaction guard: an event too deeply nested to copy makes
        `_redact_for_logging` raise, but the handler logs the marker instead, still calls the
        Processor with the original event and returns its result.
        """

        global_vars = LambdaGlobals()
        global_vars.processor = None

        processor_class = MagicMock()
        lambda_handler = get_lambda_handler(processor_class, global_vars, self.TEST_CONFIG)

        deep_event = []
        cursor = deep_event
        for _ in range(2000):
            nested = []
            cursor.append(nested)
            cursor = nested

        with self.assertRaises(RecursionError):
            _redact_for_logging(deep_event)

        result = lambda_handler(event=deep_event, context=MagicMock())

        processor_class.assert_called_once()
        processor_class.return_value.assert_called_once_with(deep_event)
        self.assertIs(result, processor_class.return_value.return_value)
        self.assertIn(call(LOG_REDACTED_VALUE), mock_logger_info.call_args_list)


    @patch.object(logger, 'warning')
    def test__log_redacted__unredactable_value_logs_marker_and_warns(self, mock_logger_warning):
        """
        The handler-level redaction guard catches more than recursion: a raw query string with a
        lone surrogate cannot be re-encoded, `_redact_for_logging` raises `UnicodeEncodeError`,
        and `_log_redacted` returns the marker and warns naming only the exception type.
        """

        event = {'rawQueryString': 'x=' + chr(0xD800)}

        with self.assertRaises(UnicodeEncodeError):
            _redact_for_logging(event)

        self.assertEqual(_log_redacted(event), LOG_REDACTED_VALUE)
        mock_logger_warning.assert_called_once_with(
            "Could not redact the value for logging (%s), logging the redaction marker instead",
            'UnicodeEncodeError')


    @patch.object(logger, 'warning')
    @patch.object(logger, 'info')
    def test_lambda_handler__unredactable_event_still_calls_processor(self, mock_logger_info,
                                                                      mock_logger_warning):
        """
        An event whose redacted copy cannot be built (the lone surrogate of the test above) never
        fails the invocation: the handler logs the marker, the Processor is still called with the
        original event object and its result object is returned to the caller as is.
        """

        global_vars = LambdaGlobals()
        global_vars.processor = None

        processor_class = MagicMock()
        lambda_handler = get_lambda_handler(processor_class, global_vars, self.TEST_CONFIG)

        event = {'rawQueryString': 'x=' + chr(0xD800)}
        result = lambda_handler(event=event, context=MagicMock())

        processor_class.assert_called_once()
        processor_class.return_value.assert_called_once_with(event)
        self.assertIs(result, processor_class.return_value.return_value)
        self.assertIn(call(LOG_REDACTED_VALUE), mock_logger_info.call_args_list)
        mock_logger_warning.assert_called_once_with(
            "Could not redact the value for logging (%s), logging the redaction marker instead",
            'UnicodeEncodeError')


    def test__redact_for_logging__api_gateway_v1_proxy_event(self):
        """
        REST API (v1) proxy event: headers, multiValueHeaders, query params and nested authorizer
        claims. Sensitive keys are masked at any depth; non-sensitive claim keys survive.
        """

        event = {
            'httpMethod':            'GET',
            'path':                  '/things',
            'headers':               {
                'Authorization':   'test-jwt-value',
                'X-Origin-Verify': 'test-edge-secret-value',
                'Cookie':          'session=test-cookie-value',
                'X-Api-Key':       'test-api-key-value',
                'Content-Type':    'application/json',
            },
            'multiValueHeaders':     {
                'Authorization':   ['test-jwt-value'],
                'X-Origin-Verify': ['test-edge-secret-value'],
                'Cookie':          ['session=test-cookie-value', 'theme=dark'],
                'Content-Type':    ['application/json'],
            },
            'queryStringParameters': {'access_token': 'test-access-token-value', 'limit': '10'},
            'requestContext':        {
                'authorizer': {'claims': {'sub': 'test-sub-claim', 'email': 'test@example.com'}},
            },
            'body':                  None,
        }
        snapshot = copy.deepcopy(event)

        redacted = _redact_for_logging(event)

        self.assertEqual(event, snapshot, "Input must not be mutated")
        self.assertEqual(redacted['httpMethod'], 'GET')
        self.assertEqual(redacted['path'], '/things')
        self.assertEqual(redacted['headers'], {
            'Authorization':   LOG_REDACTED_VALUE,
            'X-Origin-Verify': LOG_REDACTED_VALUE,
            'Cookie':          LOG_REDACTED_VALUE,
            'X-Api-Key':       LOG_REDACTED_VALUE,
            'Content-Type':    'application/json',
        })
        self.assertEqual(redacted['multiValueHeaders']['Authorization'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['multiValueHeaders']['X-Origin-Verify'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['multiValueHeaders']['Cookie'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['multiValueHeaders']['Content-Type'], ['application/json'])
        self.assertEqual(redacted['queryStringParameters'], {'access_token': LOG_REDACTED_VALUE, 'limit': '10'})
        self.assertEqual(redacted['requestContext']['authorizer']['claims'],
                         {'sub': 'test-sub-claim', 'email': 'test@example.com'})
        self.assertIsNone(redacted['body'])


    def test__redact_for_logging__http_api_v2_and_token_authorizer(self):
        """
        HTTP API (v2) carries `cookies` as a top-level list; a TOKEN authorizer event carries the
        raw token in `authorizationToken`. Both must be masked.
        """

        v2_event = {
            'version':               '2.0',
            'routeKey':              'GET /things',
            'cookies':               ['session=test-cookie-value', 'theme=dark'],
            'headers':               {'authorization': 'test-jwt-value'},
            'queryStringParameters': {'access_token': 'test-access-token-value'},
            'rawQueryString':        'access_token=test-access-token-value&limit=10',
        }
        snapshot = copy.deepcopy(v2_event)

        redacted = _redact_for_logging(v2_event)

        self.assertEqual(v2_event, snapshot, "Input must not be mutated")
        self.assertEqual(redacted['version'], '2.0')
        self.assertEqual(redacted['cookies'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['headers'], {'authorization': LOG_REDACTED_VALUE})
        self.assertEqual(redacted['queryStringParameters'], {'access_token': LOG_REDACTED_VALUE})
        self.assertEqual(redacted['rawQueryString'], f'access_token={LOG_REDACTED_VALUE}&limit=10')

        auth_event = {
            'type':               'TOKEN',
            'authorizationToken': 'test-jwt-value',
            'methodArn':          'arn:aws:execute-api:us-east-1:123456789012:api/GET/things',
        }
        snapshot = copy.deepcopy(auth_event)

        redacted = _redact_for_logging(auth_event)

        self.assertEqual(auth_event, snapshot, "Input must not be mutated")
        self.assertEqual(redacted['type'], 'TOKEN')
        self.assertEqual(redacted['authorizationToken'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['methodArn'], auth_event['methodArn'])


    def test__redact_for_logging__mixed_case_and_non_string_keys(self):
        data = {
            'AUTHORIZATION':        'test-jwt-value',
            'x-origin-verify':      'test-edge-secret-value',
            'Proxy-Authorization':  'test-proxy-auth-value',
            42:                     {'Authorization': 'test-jwt-value'},
            'path':                 '/things',
        }
        snapshot = copy.deepcopy(data)

        redacted = _redact_for_logging(data)

        self.assertEqual(data, snapshot, "Input must not be mutated")
        self.assertIn(42, redacted)
        self.assertEqual(redacted[42], {'Authorization': LOG_REDACTED_VALUE})
        self.assertEqual(redacted['AUTHORIZATION'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['x-origin-verify'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['Proxy-Authorization'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['path'], '/things')


    def test__redact_for_logging__containers_and_scalars(self):
        """
        Lists and tuples are rebuilt recursively; only dict KEYS are matched, values like a plain
        string are kept; non-container leaves are returned as the same object.
        """

        data = [
            [{'X-Amz-Security-Token': 'test-sts-token-value'}, {'signature_check': 'checked'}],
            ('X-Amz-Signature', 'public'),
            None,
        ]
        snapshot = copy.deepcopy(data)

        redacted = _redact_for_logging(data)

        self.assertEqual(data, snapshot, "Input must not be mutated")
        self.assertEqual(redacted, [
            [{'X-Amz-Security-Token': LOG_REDACTED_VALUE}, {'signature_check': LOG_REDACTED_VALUE}],
            ('X-Amz-Signature', 'public'),
            None,
        ])
        self.assertIsInstance(redacted[1], tuple)

        for scalar in ('plain', None, 42, 4.2):
            self.assertIs(_redact_for_logging(scalar), scalar)


    def test__redact_for_logging__numeric_counters_kept(self):
        """
        Sensitive keys keep None and boolean values, and numbers of counter-like keys that have a
        whole word of `LOG_COUNTER_KEY_WORDS` - separator and camelCase variants included. Keys
        like `account_password` or `otp_token` have no counter word, so their numbers are masked
        together with str, list and bytes values.
        """

        data = {
            'usage':                  {'input_tokens': 1200},
            'max_tokens':             4096,
            'token_count':            3,
            'tokenCount':             96,
            'inputTokens':            500,
            'price_tokens':           Decimal('1.5'),
            'token_valid':            True,
            'next_token':             None,
            'password':               12345678,
            'otp_token':              482913,
            'account_password':       482913,
            'service_account_secret': 12345678,
            'access_token':           'test-access-token-value',
            'refresh_token':          ['x'],
            'api_key':                b'test-bytes',
        }
        snapshot = copy.deepcopy(data)

        redacted = _redact_for_logging(data)

        self.assertEqual(data, snapshot, "Input must not be mutated")
        self.assertEqual(redacted['usage'], {'input_tokens': 1200})
        self.assertEqual(redacted['max_tokens'], 4096)
        self.assertEqual(redacted['token_count'], 3)
        self.assertEqual(redacted['tokenCount'], 96)
        self.assertEqual(redacted['inputTokens'], 500)
        self.assertEqual(redacted['price_tokens'], Decimal('1.5'))
        self.assertIs(redacted['token_valid'], True)
        self.assertIsNone(redacted['next_token'])
        self.assertEqual(redacted['password'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['otp_token'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['account_password'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['service_account_secret'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['access_token'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['refresh_token'], LOG_REDACTED_VALUE)
        self.assertEqual(redacted['api_key'], LOG_REDACTED_VALUE)


    def test__redact_for_logging__raw_query_string(self):
        """
        HTTP API (v2) / Function URL events repeat the unparsed query in `rawQueryString`:
        parameters with sensitive names are masked inside the string (case-insensitive match on
        the parameter name), non-sensitive ones keep their values, an empty string stays empty.
        """

        data = {'rawQueryString': 'access_token=test-access-token-value&limit=10&API_KEY=test-api-key-value'}
        snapshot = copy.deepcopy(data)

        redacted = _redact_for_logging(data)

        self.assertEqual(data, snapshot, "Input must not be mutated")
        self.assertEqual(redacted['rawQueryString'],
                         f'access_token={LOG_REDACTED_VALUE}&limit=10&API_KEY={LOG_REDACTED_VALUE}')
        self.assertEqual(_redact_for_logging({'rawQueryString': ''})['rawQueryString'], '')


    def test__redact_for_logging__json_body_redacted(self):
        """
        A string `body` carrying a JSON object or array is parsed, redacted recursively and
        re-serialized for the log; non-sensitive fields survive and the logged copy stays a string.
        """

        object_body = json.dumps({
            'username': 'test-user',
            'password': 'test-password-value',
            'nested':   {'refresh_token': 'test-refresh-token-value'},
        })
        data = {'body': object_body, 'isBase64Encoded': False}
        snapshot = copy.deepcopy(data)

        redacted = _redact_for_logging(data)

        self.assertEqual(data, snapshot, "Input must not be mutated")
        self.assertIsInstance(redacted['body'], str)
        self.assertEqual(json.loads(redacted['body']), {
            'username': 'test-user',
            'password': LOG_REDACTED_VALUE,
            'nested':   {'refresh_token': LOG_REDACTED_VALUE},
        })

        array_body = json.dumps([{'api_key': 'test-api-key-value'}, {'path': '/things'}])
        self.assertEqual(json.loads(_redact_for_logging({'body': array_body})['body']),
                         [{'api_key': LOG_REDACTED_VALUE}, {'path': '/things'}])


    def test__redact_for_logging__plain_text_body_kept_as_is(self):
        """
        Bodies with nothing to redact are logged unchanged: plain text (spaces make both the JSON
        and the form shape fail), a `--`-prefixed body with no `content-disposition:` in it (the
        multipart shape needs both), and an empty body.
        """

        matrix = [
            {'body': 'some plain text with spaces'},
            {'body': 'some plain text', 'headers': {'Content-Type': 'application/json'}},
            {'body': '--dash-prefixed note with no disposition header'},
            {'body': ''},
        ]

        for data in matrix:
            with self.subTest(data=data):
                snapshot = copy.deepcopy(data)

                redacted = _redact_for_logging(data)

                self.assertEqual(data, snapshot, "Input must not be mutated")
                self.assertEqual(redacted['body'], data['body'])


    def test__redact_for_logging__malformed_json_body_masked(self):
        """
        A body that looks like JSON (stripped form starts with `{` or `[`) but does not parse is
        replaced with the marker instead of being logged verbatim — a truncated payload may still
        carry secrets. The declared content type and the headers key case make no difference, and
        a form-shaped body is not rescued by the form path either: `parse_qsl` would split at the
        first `=` and leave the secret inside a parameter *name*, which only value-masking covers.
        """

        matrix = [
            {'body': '{"password": "test-password-value", oops'},
            {'body': '{"password": "test-password-value", oops', 'headers': {'Content-Type': 'application/json'}},
            {'body': '["password", "test-password-value"'},
            {'body': '["password", "test-password-value"', 'Headers': {'Content-Type': 'APPLICATION/JSON'}},
            {'body': '[password=test-form-password&x=1', 'headers': {'Content-Type': 'application/json'}},
        ]
        for body in ('{"password":"test-hunter2="',
                     '{"client_secret":"test-s3cr3t","redirect":"https://x/?a=1"',
                     '{"token":"test-eyJabc.def=="'):
            matrix += [
                {'body': body},
                {'body': body, 'headers': {'content-type': 'application/json'}},
                {'body': body, 'headers': {'content-type': 'application/x-www-form-urlencoded'}},
            ]

        for data in matrix:
            with self.subTest(data=data):
                snapshot = copy.deepcopy(data)

                redacted = _redact_for_logging(data)

                self.assertEqual(data, snapshot, "Input must not be mutated")
                self.assertEqual(redacted['body'], LOG_REDACTED_VALUE)
                for secret in ('test-form-password', 'test-hunter2', 'test-s3cr3t', 'test-eyJabc'):
                    self.assertNotIn(secret, str(redacted['body']), "Secret leaked into the logged body")


    def test__redact_for_logging__multipart_body_masked(self):
        """
        A body whose sibling headers declare a `multipart/` content type (any headers-key or
        header-name case, value with parameters or surrounding whitespace) is replaced with the
        marker entirely — the payload is opaque to the redactor, even when it happens to be
        parseable JSON. Without any headers, the multipart shape alone (stripped body starts with
        `--` and contains `content-disposition:` in any case) masks the body too.
        """

        multipart_body = ('--boundary\r\n'
                          'Content-Disposition: form-data; name="password"\r\n\r\n'
                          'test-multipart-password\r\n'
                          '--boundary--')
        matrix = [
            {'body': multipart_body, 'headers': {'Content-Type': 'multipart/form-data; boundary=boundary'}},
            {'body': multipart_body, 'headers': {'content-type': 'MULTIPART/MIXED'}},
            {'body': multipart_body, 'Headers': {'CONTENT-TYPE': 'multipart/related'}},
            {'body': multipart_body, 'headers': {'Content-Type': ' multipart/form-data; boundary=x'}},
            {'body': multipart_body},
            {'body': ' ' + multipart_body.lower()},
            {'body': json.dumps({'password': 'test-password-value'}),
             'headers': {'Content-Type': 'multipart/form-data'}},
        ]

        for data in matrix:
            with self.subTest(data=data):
                snapshot = copy.deepcopy(data)

                redacted = _redact_for_logging(data)

                self.assertEqual(data, snapshot, "Input must not be mutated")
                self.assertEqual(redacted['body'], LOG_REDACTED_VALUE)
                self.assertEqual({k: v for k, v in redacted.items() if k != 'body'},
                                 {k: v for k, v in data.items() if k != 'body'})


    def test__redact_for_logging__form_body_redacted(self):
        """
        Form-encoded bodies are redacted like query strings: when the sibling headers declare the
        `application/x-www-form-urlencoded` content type (any key or header-name case, value with
        parameters or surrounding whitespace), when the body has the `k=v&k=v` shape on its own,
        and even when a sibling content type says otherwise. A body that looks like JSON but does
        not parse is never form-redacted — it is pinned to the marker by
        `test__redact_for_logging__malformed_json_body_masked`.
        """

        form_body = 'grant_type=password&password=test-form-password'
        redacted_form = f'grant_type=password&password={LOG_REDACTED_VALUE}'
        matrix = [
            ({'body': form_body, 'headers': {'content-type': 'application/x-www-form-urlencoded'}}, redacted_form),
            ({'body': form_body, 'headers': {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}},
             redacted_form),
            ({'body': form_body, 'Headers': {'Content-Type': 'APPLICATION/X-WWW-FORM-URLENCODED'}}, redacted_form),
            ({'body': form_body, 'headers': {'Content-Type': '\tapplication/x-www-form-urlencoded'}}, redacted_form),
            ({'body': 'password=test form password', 'headers': {'Content-Type': 'application/x-www-form-urlencoded'}},
             f'password={LOG_REDACTED_VALUE}'),
            ({'body': form_body}, redacted_form),
            ({'body': 'password=secret-value', 'headers': {'Content-Type': 'application/json'}},
             f'password={LOG_REDACTED_VALUE}'),
        ]

        for data, expected in matrix:
            with self.subTest(data=data):
                snapshot = copy.deepcopy(data)

                redacted = _redact_for_logging(data)

                self.assertEqual(data, snapshot, "Input must not be mutated")
                self.assertEqual(redacted['body'], expected)


    def test__redact_for_logging__base64_body_masked(self):
        """
        A body of a dict with a truthy `isBase64Encoded` is replaced with the marker: base64 is
        reversible, so it is never logged, JSON-shaped or not.
        """

        for body in ('dXNlcjpwYXNzd29yZA==', json.dumps({'password': 'test-password-value'})):
            with self.subTest(body=body):
                data = {'body': body, 'isBase64Encoded': True}
                snapshot = copy.deepcopy(data)

                redacted = _redact_for_logging(data)

                self.assertEqual(data, snapshot, "Input must not be mutated")
                self.assertEqual(redacted['body'], LOG_REDACTED_VALUE)


    def test__redact_for_logging__deeply_nested_body_masked(self):
        """
        JSON bodies nested 2000 deep cannot be redacted recursively, so the logged copy carries the
        marker for them instead - of both the list and the dict nesting shapes.
        """

        for shape, deep_body in (('list', '[' * 2000 + ']' * 2000), ('dict', '{"a":' * 2000 + '1' + '}' * 2000)):
            with self.subTest(shape=shape):
                data = {'body': deep_body, 'headers': {'Content-Type': 'application/json'}}
                snapshot = copy.deepcopy(data)

                redacted = _redact_for_logging(data)

                self.assertEqual(data, snapshot, "Input must not be mutated")
                self.assertEqual(redacted['body'], LOG_REDACTED_VALUE)
                self.assertEqual(redacted['headers'], {'Content-Type': 'application/json'})


    def test__redact_for_logging__json_body_keeps_non_ascii(self):
        """
        Re-serialized bodies keep non-ASCII characters readable (`ensure_ascii=False`).
        """

        body = json.dumps({'name': 'Ωμέγα', 'password': 'test-password-value'})
        data = {'body': body}

        redacted = _redact_for_logging(data)

        self.assertIn('Ωμέγα', redacted['body'])
        self.assertNotIn('\\u03a9', redacted['body'])
        self.assertEqual(json.loads(redacted['body']), {'name': 'Ωμέγα', 'password': LOG_REDACTED_VALUE})


    def test__redact_for_logging__sensitive_parts_read_at_call_time(self):
        """
        Reassigning `sosw.app.LOG_SENSITIVE_KEY_PARTS` extends the matching for subsequent calls.
        """

        original = sosw.app.LOG_SENSITIVE_KEY_PARTS
        self.addCleanup(setattr, sosw.app, 'LOG_SENSITIVE_KEY_PARTS', original)

        sosw.app.LOG_SENSITIVE_KEY_PARTS = original + ('custom',)

        self.assertEqual(_redact_for_logging({'custom-key': 'test-custom-secret-value', 'path': '/x'}),
                         {'custom-key': LOG_REDACTED_VALUE, 'path': '/x'})
        self.assertEqual(_redact_for_logging({'Authorization': 'test-jwt-value'}),
                         {'Authorization': LOG_REDACTED_VALUE})


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
        p.stats['function_name'] = 'some_function'

        p.reset_stats()

        self.assertEqual(p.stats['total_numeric'], 5)
        self.assertNotIn('function_name', p.stats)
        self.assertNotIn('total_function_name', p.stats)


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
