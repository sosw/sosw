"""
Unit tests for ``{{ cookiecutter.function_name }}``.

Pure unit tests: boto3 and the sosw config lookup are mocked, no network calls.
Run from the project root: ``python -m pytest test/ -q`` (or ``python test/test_unit.py``).
"""

{% if cookiecutter.use_lambda_api == 'yes' %}
import json
{% endif %}
import os
import sys
import unittest

from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

os.environ['STAGE'] = 'test'
os.environ['autotest'] = 'True'

from app import Processor

{% if cookiecutter.use_lambda_api == 'yes' %}

CLAIMS = {'sub': 'test-user'}


def make_apigw_event(path, method='GET', claims=None):
    """
    Build a minimal API Gateway HTTP API (payload v2) proxy event.

    :param str path:        Request path, e.g. ``'/things/42'``.
    :param str method:      HTTP method.
    :param dict claims:     Cognito claims of the caller, or None for an unauthenticated request.
    :rtype:                 dict
    """

    event = {
        'rawPath':        path,
        'requestContext': {'http': {'method': method}},
    }

    if claims is not None:
        event['requestContext']['authorizer'] = {'jwt': {'claims': claims}}

    return event


class ProcessorTestCase(unittest.TestCase):

    def setUp(self):
        self.patcher_boto = patch('boto3.client')
        self.mock_boto_client = self.patcher_boto.start()
        self.mock_boto_client.return_value = MagicMock()

        with patch.object(Processor, 'get_config', return_value={}):
            self.processor = Processor(custom_config={'things': {'1': 'one', '2': 'two'}}, test=True)


    def tearDown(self):
        self.patcher_boto.stop()
        del self.processor


    def test_processor_initialization(self):
        self.assertIsInstance(self.processor, Processor)


    def test_health_route(self):
        response = self.processor(make_apigw_event('/health', claims=CLAIMS))

        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body']), {'status': 'ok'})


    def test_missing_claims_return_401(self):
        response = self.processor(make_apigw_event('/health'))

        self.assertEqual(response['statusCode'], 401)


    def test_unknown_route_returns_404(self):
        response = self.processor(make_apigw_event('/nowhere', claims=CLAIMS))

        self.assertEqual(response['statusCode'], 404)


    def test_get_thing_extracts_path_parameter(self):
        response = self.processor(make_apigw_event('/things/42', claims=CLAIMS))

        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body']), {'thing_id': '42', 'thing': 'The Answer'})


    def test_get_thing_missing_returns_404(self):
        response = self.processor(make_apigw_event('/things/13', claims=CLAIMS))

        self.assertEqual(response['statusCode'], 404)
        self.assertEqual(json.loads(response['body'])['error']['code'], 'NOT_FOUND')


    def test_no_state_leak_between_invocations(self):
        """Simulate a warm container: the second response must reflect only the second request."""
        self.processor(make_apigw_event('/things/1', claims=CLAIMS))
        response = self.processor(make_apigw_event('/things/2', claims=CLAIMS))

        self.assertEqual(json.loads(response['body']), {'thing_id': '2', 'thing': 'two'})


    def test_no_claims_leak_between_invocations(self):
        """Simulate a warm container: caller identity must not survive into the next invocation."""
        self.processor(make_apigw_event('/health', claims=CLAIMS))
        response = self.processor(make_apigw_event('/health'))

        self.assertEqual(response['statusCode'], 401)
{% else %}

class ProcessorTestCase(unittest.TestCase):

    def setUp(self):
        self.patcher_boto = patch('boto3.client')
        self.mock_boto_client = self.patcher_boto.start()
        self.mock_boto_client.return_value = MagicMock()

        with patch.object(Processor, 'get_config', return_value={}):
            self.processor = Processor(test=True)


    def tearDown(self):
        self.patcher_boto.stop()
        del self.processor


    def test_processor_initialization(self):
        self.assertIsInstance(self.processor, Processor)


    def test_call_counts_processed_records(self):
        result = self.processor({'records': ['a', 'b']})

        self.assertEqual(result['processed'], 2)


    def test_call_without_records(self):
        result = self.processor({})

        self.assertEqual(result['processed'], 0)


    def test_no_state_leak_between_invocations(self):
        """Simulate a warm container: second call must not see first call's state."""
        self.processor({'records': ['a', 'b', 'c']})
        result = self.processor({'records': ['d']})

        self.assertEqual(result['processed'], 1)
{% endif %}


if __name__ == '__main__':
    unittest.main()
