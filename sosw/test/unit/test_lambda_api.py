import importlib
import json
import os
import sys
import time
import unittest

from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.app import LambdaGlobals, get_lambda_handler
from sosw.components.exceptions import ApiError, BadRequestError, ConflictError, ForbiddenError, NotFoundError, \
    ServerError, UnauthorizedError
from sosw.lambda_api import LambdaApi


class lambda_api_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = {
        'auth_enabled': False,
        'routes':       {
            'GET /things/{thing_id}': 'get_thing',  # Declared before the static routes on purpose.
            'GET /things':            'list_things',
            'GET /things/count':      'count_things',
            'POST /things':           'create_thing',
            'GET /files/{proxy+}':    'get_file',
            'GET /types':             'get_types',
            'GET /boom':              'boom',
            'GET /error':             'raise_configured_error',
        },
    }

    AUTH_CONFIG = {
        'auth_enabled': True,
        'routes':       {'GET /things': 'list_things'},
    }

    GROUPS_CONFIG = {
        'auth_enabled':      True,
        'authorized_groups': ['admins'],
        'routes':            {'GET /things': 'list_things'},
    }


    class ThingsApi(LambdaApi):

        error_to_raise = None


        def list_things(self, event, **kwargs):
            return {'things': ['red', 'green']}


        def count_things(self, event, **kwargs):
            return {'count': 2}


        def get_thing(self, event, thing_id=None, **kwargs):
            return {'thing_id': thing_id}


        def create_thing(self, event, **kwargs):
            return self.make_response({'created': True}, status_code=201, headers={'x-total-count': '1'})


        def get_file(self, event, proxy=None, **kwargs):
            return {'proxy': proxy}


        def get_types(self, event, **kwargs):
            return {
                'price':      Decimal('9.99'),
                'quantity':   Decimal('3'),
                'created_at': datetime(2026, 7, 4, 12, 30, 0),
                'tags':       {'beta', 'alpha'},
            }


        def boom(self, event, **kwargs):
            raise RuntimeError("Database password is hunter2")


        def raise_configured_error(self, event, **kwargs):
            raise self.error_to_raise


    def setUp(self):
        self.patcher = patch.object(LambdaApi, 'get_config', MagicMock(return_value={}))
        self.get_config_patch = self.patcher.start()


    def tearDown(self):
        self.patcher.stop()

        # Reset the container-level globals of sosw.app that get_lambda_handler tests may have populated.
        reset_globals = LambdaGlobals()
        reset_globals.processor = None


    def make_processor(self, custom_config=None):
        config = deepcopy(custom_config if custom_config is not None else self.TEST_CONFIG)
        return self.ThingsApi(custom_config=config)


    @staticmethod
    def make_v1_event(method='GET', path='/things', claims=None):
        """Trimmed API Gateway REST API (payload version 1.0) proxy event."""

        event = {
            'resource':              path,
            'path':                  path,
            'httpMethod':            method,
            'headers':               {'Accept': 'application/json', 'Host': 'api.example.com'},
            'queryStringParameters': None,
            'pathParameters':        None,
            'requestContext':        {
                'resourcePath': path,
                'httpMethod':   method,
                'stage':        'prod',
                'identity':     {'sourceIp': '192.0.2.10'},
            },
            'body':                  None,
            'isBase64Encoded':       False,
        }
        if claims is not None:
            event['requestContext']['authorizer'] = {'claims': claims}
        return event


    @staticmethod
    def make_v2_event(method='GET', path='/things', claims=None):
        """Trimmed API Gateway HTTP API (payload version 2.0) proxy event."""

        event = {
            'version':         '2.0',
            'routeKey':        f'{method} {path}',
            'rawPath':         path,
            'rawQueryString':  '',
            'headers':         {'accept': 'application/json', 'host': 'api.example.com'},
            'requestContext':  {
                'accountId': '123456789012',
                'apiId':     'a1b2c3d4e5',
                'stage':     '$default',
                'http':      {
                    'method':    method,
                    'path':      path,
                    'protocol':  'HTTP/1.1',
                    'sourceIp':  '192.0.2.10',
                    'userAgent': 'pytest',
                },
            },
            'body':            None,
            'isBase64Encoded': False,
        }
        if claims is not None:
            event['requestContext']['authorizer'] = {'jwt': {'claims': claims, 'scopes': None}}
        return event


    @staticmethod
    def make_claims(**overrides):
        """Claims of a Cognito id token, in the shape the API Gateway authorizer delivers them."""

        claims = {
            'sub':              '5f95d472-1234-5678-9abc-def012345678',
            'aud':              '6q1c4ff87a1p3l6nkl2vd0e2e8',
            'token_use':        'id',
            'cognito:username': 'jane',
            'email':            'jane@example.com',
            'exp':              int(time.time()) + 3600,
            'iat':              int(time.time()) - 60,
        }
        claims.update(overrides)
        return claims


    # ------------------------------------------------------------------ routing

    def test_call__routes_to_handler__v1(self):
        processor = self.make_processor()
        response = processor(self.make_v1_event(path='/things'))

        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body']), {'things': ['red', 'green']})


    def test_call__unknown_route__404(self):
        processor = self.make_processor()
        response = processor(self.make_v1_event(path='/nope'))

        self.assertEqual(response['statusCode'], 404)
        body = json.loads(response['body'])
        self.assertEqual(body['error']['code'], 'NOT_FOUND')
        self.assertIn('GET /nope', body['error']['message'])


    def test_call__wrong_method__404(self):
        processor = self.make_processor()
        response = processor(self.make_v1_event(method='DELETE', path='/things'))

        self.assertEqual(response['statusCode'], 404)
        self.assertEqual(json.loads(response['body'])['error']['code'], 'NOT_FOUND')


    def test_call__static_route_wins_over_parametrized(self):
        processor = self.make_processor()
        response = processor(self.make_v1_event(path='/things/count'))

        self.assertEqual(json.loads(response['body']), {'count': 2})


    def test_call__trailing_slash_and_query_string_normalized(self):
        processor = self.make_processor()

        event = self.make_v1_event(path='/things/')
        event['queryStringParameters'] = {'color': 'red'}
        self.assertEqual(processor(event)['statusCode'], 200)

        # Defensive: some proxies keep the query string glued to the path.
        response = processor(self.make_v1_event(path='/things?color=red'))
        self.assertEqual(response['statusCode'], 200)


    def test_call__path_prefix_stripped(self):
        config = deepcopy(self.TEST_CONFIG)
        config['path_prefixes'] = ['/api']
        processor = self.make_processor(config)

        response = processor(self.make_v1_event(path='/api/things'))
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body']), {'things': ['red', 'green']})


    def test_get_router__built_once_per_container(self):
        processor = self.make_processor()

        with patch.object(processor, 'build_router', wraps=processor.build_router) as build_spy:
            processor(self.make_v1_event(path='/things'))
            processor(self.make_v1_event(path='/things/42'))

        self.assertEqual(build_spy.call_count, 1)


    def test_build_router__invalid_configurations_raise(self):
        processor = self.make_processor({'auth_enabled': False, 'routes': {'GET /nope': 'no_such_method'}})
        self.assertRaises(ValueError, processor.build_router)

        # A misconfigured router must surface as a logged 500, never as a broken integration response.
        response = processor(self.make_v1_event(path='/nope'))
        self.assertEqual(response['statusCode'], 500)

        processor = self.make_processor({'auth_enabled': False, 'routes': {'GETthings': 'list_things'}})
        self.assertRaises(ValueError, processor.build_router)


    # ------------------------------------------------------------------ event shapes

    def test_call__v2_event__routes_to_handler(self):
        processor = self.make_processor()
        response = processor(self.make_v2_event(path='/things'))

        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body']), {'things': ['red', 'green']})


    def test_call__route_key_only_event(self):
        processor = self.make_processor()
        response = processor({'routeKey': 'GET /things', 'requestContext': {}})

        self.assertEqual(response['statusCode'], 200)


    def test_call__non_api_event__400(self):
        processor = self.make_processor()
        response = processor({'Records': [{'eventSource': 'aws:sqs'}]})

        self.assertEqual(response['statusCode'], 400)
        self.assertEqual(json.loads(response['body'])['error']['code'], 'BAD_REQUEST')


    # ------------------------------------------------------------------ path parameters

    def test_call__path_params_passed_to_handler(self):
        processor = self.make_processor()

        response = processor(self.make_v1_event(path='/things/42'))
        self.assertEqual(json.loads(response['body']), {'thing_id': '42'})

        # Values are URL-decoded.
        response = processor(self.make_v2_event(path='/things/hello%20world'))
        self.assertEqual(json.loads(response['body']), {'thing_id': 'hello world'})


    def test_call__greedy_proxy_path_params(self):
        processor = self.make_processor()
        response = processor(self.make_v1_event(path='/files/reports/2026/07/data.csv'))

        self.assertEqual(json.loads(response['body']), {'proxy': 'reports/2026/07/data.csv'})


    # ------------------------------------------------------------------ authorization

    def test_get_claims__v1_v2_and_missing(self):
        claims = self.make_claims()

        self.assertEqual(LambdaApi.get_claims(self.make_v1_event(claims=claims)), claims)
        self.assertEqual(LambdaApi.get_claims(self.make_v2_event(claims=claims)), claims)
        self.assertEqual(LambdaApi.get_claims(self.make_v1_event()), {})
        self.assertEqual(LambdaApi.get_claims({}), {})

        # A crafted non-dict authorizer must degrade to empty claims (401 path), not crash to a 500.
        self.assertEqual(LambdaApi.get_claims({'requestContext': {'authorizer': 'crafted-string'}}), {})
        self.assertEqual(LambdaApi.get_claims({'requestContext': {'authorizer': ['crafted', 'list']}}), {})


    def test_call__auth_required__missing_claims__401(self):
        processor = self.make_processor(self.AUTH_CONFIG)
        response = processor(self.make_v1_event(path='/things'))

        self.assertEqual(response['statusCode'], 401)
        body = json.loads(response['body'])
        self.assertEqual(body['error']['code'], 'UNAUTHORIZED')
        self.assertEqual(body['error']['message'], 'Unauthorized')


    def test_call__auth_required__valid_claims__handler_runs(self):
        processor = self.make_processor(self.AUTH_CONFIG)
        claims = self.make_claims()
        response = processor(self.make_v1_event(path='/things', claims=claims))

        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(processor.claims, claims)


    def test_call__auth_required__v2_claims__handler_runs(self):
        processor = self.make_processor(self.AUTH_CONFIG)
        claims = self.make_claims(exp=str(int(time.time()) + 3600))  # v2 authorizers stringify numbers.
        response = processor(self.make_v2_event(path='/things', claims=claims))

        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(processor.claims, claims)


    def test_call__expired_token__401_without_claim_leak(self):
        processor = self.make_processor(self.AUTH_CONFIG)
        claims = self.make_claims(exp=int(time.time()) - 100)
        response = processor(self.make_v1_event(path='/things', claims=claims))

        self.assertEqual(response['statusCode'], 401)
        self.assertNotIn(claims['email'], response['body'])
        self.assertNotIn(claims['sub'], response['body'])


    def test_token_expired__format_tolerance(self):
        now = time.time()

        self.assertTrue(LambdaApi.token_expired(int(now) - 100))
        self.assertFalse(LambdaApi.token_expired(int(now) + 100))
        self.assertTrue(LambdaApi.token_expired(now - 100.5))
        self.assertTrue(LambdaApi.token_expired(str(int(now) - 100)))
        self.assertFalse(LambdaApi.token_expired(str(int(now) + 100)))

        # Legacy custom-authorizer format.
        self.assertTrue(LambdaApi.token_expired('Wed Aug 06 09:02:12 UTC 2025'))
        self.assertFalse(LambdaApi.token_expired('Mon Jan 01 00:00:00 UTC 2125'))

        # Missing or unparsable claims never crash and are not treated as expired.
        self.assertFalse(LambdaApi.token_expired(None))
        self.assertFalse(LambdaApi.token_expired('garbage'))
        self.assertFalse(LambdaApi.token_expired({'weird': 'type'}))


    def test_call__wrong_group__403(self):
        processor = self.make_processor(self.GROUPS_CONFIG)

        claims = self.make_claims(**{'cognito:groups': ['users']})
        response = processor(self.make_v1_event(path='/things', claims=claims))
        self.assertEqual(response['statusCode'], 403)
        self.assertEqual(json.loads(response['body'])['error']['code'], 'FORBIDDEN')

        # Missing groups claim is also a 403 when groups are required.
        response = processor(self.make_v1_event(path='/things', claims=self.make_claims()))
        self.assertEqual(response['statusCode'], 403)


    def test_call__authorized_group__handler_runs(self):
        processor = self.make_processor(self.GROUPS_CONFIG)

        claims = self.make_claims(**{'cognito:groups': ['admins', 'users']})
        response = processor(self.make_v1_event(path='/things', claims=claims))
        self.assertEqual(response['statusCode'], 200)

        # HTTP API v2 delivers the groups claim as a stringified list.
        claims = self.make_claims(**{'cognito:groups': '[admins users]'})
        response = processor(self.make_v2_event(path='/things', claims=claims))
        self.assertEqual(response['statusCode'], 200)


    def test_parse_groups__variants(self):
        self.assertEqual(LambdaApi.parse_groups(None), [])
        self.assertEqual(LambdaApi.parse_groups([]), [])
        self.assertEqual(LambdaApi.parse_groups(['admins', 'users']), ['admins', 'users'])
        self.assertEqual(LambdaApi.parse_groups('admins'), ['admins'])
        self.assertEqual(LambdaApi.parse_groups('admins,users'), ['admins', 'users'])
        self.assertEqual(LambdaApi.parse_groups('[admins users]'), ['admins', 'users'])
        self.assertEqual(LambdaApi.parse_groups(42), ['42'])


    def test_call__auth_disabled__no_claims__handler_runs(self):
        processor = self.make_processor()
        response = processor(self.make_v1_event(path='/things'))

        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(processor.claims, {})


    def test_check_route_access__receives_route_and_claims(self):
        processor = self.make_processor()
        processor.check_route_access = MagicMock()
        claims = self.make_claims()

        processor(self.make_v1_event(path='/things/42', claims=claims))

        processor.check_route_access.assert_called_once_with('GET /things/{thing_id}', claims)


    def test_check_route_access__override_denies__403(self):
        class RestrictedApi(self.ThingsApi):

            def check_route_access(self, route, claims):
                raise ForbiddenError("Route access denied by RBAC")

        processor = RestrictedApi(custom_config=deepcopy(self.TEST_CONFIG))
        response = processor(self.make_v1_event(path='/things'))

        self.assertEqual(response['statusCode'], 403)
        self.assertEqual(json.loads(response['body'])['error']['message'], 'Route access denied by RBAC')


    # ------------------------------------------------------------------ responses

    def test_call__default_cors_headers(self):
        processor = self.make_processor()
        response = processor(self.make_v1_event(path='/things'))

        self.assertEqual(response['headers']['Access-Control-Allow-Origin'], '*')
        self.assertEqual(response['headers']['Content-Type'], 'application/json')
        self.assertIn('GET', response['headers']['Access-Control-Allow-Methods'])


    def test_call__custom_cors_headers_from_config(self):
        config = deepcopy(self.TEST_CONFIG)
        config['cors_headers'] = {'Access-Control-Allow-Origin': 'https://api.example.com'}
        processor = self.make_processor(config)

        response = processor(self.make_v1_event(path='/things'))

        self.assertEqual(response['headers']['Access-Control-Allow-Origin'], 'https://api.example.com')
        # The rest of the default headers survive the recursive config merge.
        self.assertEqual(response['headers']['Content-Type'], 'application/json')


    def test_call__error_responses_carry_cors_headers(self):
        processor = self.make_processor()
        response = processor(self.make_v1_event(path='/nope'))

        self.assertEqual(response['statusCode'], 404)
        self.assertEqual(response['headers']['Access-Control-Allow-Origin'], '*')


    def test_json_default__encoder_cases(self):
        self.assertEqual(LambdaApi.json_default(Decimal('42')), 42)
        self.assertIsInstance(LambdaApi.json_default(Decimal('42')), int)
        self.assertEqual(LambdaApi.json_default(Decimal('9.99')), 9.99)
        self.assertEqual(LambdaApi.json_default(datetime(2026, 7, 4, 12, 30)), '2026-07-04T12:30:00')
        self.assertEqual(LambdaApi.json_default(date(2026, 7, 4)), '2026-07-04')
        self.assertEqual(LambdaApi.json_default({3, 1, 2}), [1, 2, 3])
        self.assertEqual(LambdaApi.json_default(frozenset(['b', 'a'])), ['a', 'b'])
        self.assertEqual(sorted(LambdaApi.json_default({1, 'mixed'}), key=str), [1, 'mixed'])

        self.assertRaises(TypeError, LambdaApi.json_default, object())


    def test_call__body_encodes_dynamodb_types(self):
        processor = self.make_processor()
        response = processor(self.make_v1_event(path='/types'))

        body = json.loads(response['body'])
        self.assertEqual(body['price'], 9.99)
        self.assertEqual(body['quantity'], 3)
        self.assertEqual(body['created_at'], '2026-07-04T12:30:00')
        self.assertEqual(body['tags'], ['alpha', 'beta'])


    def test_make_response__none_body_and_extra_headers(self):
        processor = self.make_processor()
        response = processor.make_response(None, status_code=204, headers={'x-custom': 'yes'})

        self.assertEqual(response['statusCode'], 204)
        self.assertEqual(response['body'], '')
        self.assertEqual(response['headers']['x-custom'], 'yes')
        self.assertEqual(response['headers']['Access-Control-Allow-Origin'], '*')


    def test_call__pre_rendered_response_passthrough(self):
        processor = self.make_processor()
        response = processor(self.make_v1_event(method='POST', path='/things'))

        self.assertEqual(response['statusCode'], 201)
        self.assertEqual(response['headers']['x-total-count'], '1')
        self.assertEqual(json.loads(response['body']), {'created': True})


    # ------------------------------------------------------------------ error mapping

    def test_exceptions__contract(self):
        expected = [
            (BadRequestError, 400, 'BAD_REQUEST', 'Bad request'),
            (UnauthorizedError, 401, 'UNAUTHORIZED', 'Unauthorized'),
            (ForbiddenError, 403, 'FORBIDDEN', 'Forbidden'),
            (NotFoundError, 404, 'NOT_FOUND', 'Not found'),
            (ConflictError, 409, 'CONFLICT', 'Conflict'),
            (ServerError, 500, 'SERVER_ERROR', 'Internal server error'),
        ]

        for error_class, status_code, error_code, default_message in expected:
            error = error_class()
            self.assertIsInstance(error, ApiError)
            self.assertEqual(error.status_code, status_code)
            self.assertEqual(error.error_code, error_code)
            self.assertEqual(str(error), default_message)

        custom = ApiError("I am a teapot", status_code=418, error_code='TEAPOT')
        self.assertEqual((custom.status_code, custom.error_code, str(custom)), (418, 'TEAPOT', 'I am a teapot'))


    def test_call__api_error_subclasses_map_to_status_and_envelope(self):
        processor = self.make_processor()

        expected = [
            (BadRequestError, 400, 'BAD_REQUEST'),
            (UnauthorizedError, 401, 'UNAUTHORIZED'),
            (ForbiddenError, 403, 'FORBIDDEN'),
            (NotFoundError, 404, 'NOT_FOUND'),
            (ConflictError, 409, 'CONFLICT'),
            (ServerError, 500, 'SERVER_ERROR'),
        ]

        for error_class, status_code, error_code in expected:
            processor.error_to_raise = error_class("Something specific went wrong")
            response = processor(self.make_v1_event(path='/error'))

            body = json.loads(response['body'])
            self.assertEqual(response['statusCode'], status_code)
            self.assertEqual(body['error']['code'], error_code)
            self.assertEqual(body['error']['message'], 'Something specific went wrong')


    def test_call__unexpected_exception__500_without_internals(self):
        processor = self.make_processor()

        with patch('sosw.lambda_api.logger') as mock_logger:
            response = processor(self.make_v1_event(path='/boom'))

        self.assertEqual(response['statusCode'], 500)
        body = json.loads(response['body'])
        self.assertEqual(body['error']['code'], 'SERVER_ERROR')
        self.assertEqual(body['error']['message'], 'Internal server error')
        self.assertNotIn('hunter2', response['body'])

        # The failure must still be fully logged server-side, with the traceback.
        mock_logger.exception.assert_called_once()


    # ------------------------------------------------------------------ stats & warm container

    def test_call__stats_counters(self):
        processor = self.make_processor()

        processor(self.make_v1_event(path='/things'))
        processor(self.make_v1_event(path='/things'))
        processor(self.make_v1_event(path='/nope'))
        processor(self.make_v1_event(path='/boom'))

        self.assertEqual(processor.stats['api_calls'], 4)
        self.assertEqual(processor.stats['api_route_get_things'], 2)
        self.assertEqual(processor.stats['api_route_get_boom'], 1)
        self.assertEqual(processor.stats['api_errors_4xx'], 1)
        self.assertEqual(processor.stats['api_errors_5xx'], 1)


    def test_call__claims_reset_between_invocations(self):
        processor = self.make_processor()
        claims = self.make_claims()

        processor(self.make_v1_event(path='/things', claims=claims))
        self.assertEqual(processor.claims, claims)

        processor(self.make_v1_event(path='/things'))
        self.assertEqual(processor.claims, {})


    def test_lambda_handler__container_reuse(self):
        global_vars = LambdaGlobals()
        lambda_handler = get_lambda_handler(self.ThingsApi, global_vars, custom_config=deepcopy(self.TEST_CONFIG))

        first = lambda_handler(self.make_v1_event(path='/things'), None)
        processor = global_vars.processor
        second = lambda_handler(self.make_v1_event(path='/things/42'), None)

        self.assertEqual(first['statusCode'], 200)
        self.assertEqual(json.loads(second['body']), {'thing_id': '42'})

        # The very same processor instance served both invocations, with the router built once.
        self.assertIs(global_vars.processor, processor)
        self.assertIsInstance(global_vars.processor, self.ThingsApi)
        self.assertIsNotNone(global_vars.processor._router)
        self.assertEqual(global_vars.processor.stats['total_api_calls'], 2)


    # ------------------------------------------------------------------ module plumbing

    def test_normalize_path__variants(self):
        self.assertEqual(LambdaApi._normalize_path('/things/'), '/things')
        self.assertEqual(LambdaApi._normalize_path('/things?color=red'), '/things')
        self.assertEqual(LambdaApi._normalize_path('things'), '/things')
        self.assertEqual(LambdaApi._normalize_path(''), '/')
        self.assertEqual(LambdaApi._normalize_path('/'), '/')
        self.assertEqual(LambdaApi._normalize_path('//'), '/')


    def test_logger__powertools_branch(self):
        """
        Cover the optional aws_lambda_powertools import branch by injecting a fake package
        and reloading the module. The module is reloaded back to its real state afterwards.
        """

        import sosw.lambda_api

        self.addCleanup(importlib.reload, sosw.lambda_api)

        fake_powertools = MagicMock()
        with patch.dict(sys.modules, {'aws_lambda_powertools': fake_powertools}):
            reloaded = importlib.reload(sosw.lambda_api)

            fake_powertools.Logger.assert_called_once_with(child=True)
            self.assertEqual(reloaded.logger, fake_powertools.Logger.return_value)


if __name__ == '__main__':
    unittest.main()
