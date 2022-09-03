import os
import logging
import datetime
import unittest

from unittest.mock import Mock, patch
from sosw.components.sigv4 import AWSSigV4RequestGenerator

logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class TestAWSSigV4RequestGenerator(unittest.TestCase):

    def setUp(self):
        """
        setUp TestCase.
        """

        self.auth = AWSSigV4RequestGenerator(
                aws_service='es',
                aws_access_key_id='YOUR_KEY_ID',
                aws_secret_access_key='YOUR_SECRET',
                aws_session_token='YOUR_SESSION_TOKEN',
                aws_region='us-east-1',
                aws_host='search-service.us-east-1.es.amazonaws.com',
        )


    def tearDown(self):

        try:
            del self.auth
        except:
            pass


    def test_call(self):
        expected_result = {'Authorization': 'AWS4-HMAC-SHA256 '
                                            'Credential=YOUR_KEY_ID/20220715/us-east-1/es/aws4_request, '
                                            'SignedHeaders=host;x-amz-date;x-amz-security-token, '
                                            'Signature=5dae329a8229d51b0fdc5ae3d00c2d2d26cfe9d01fd8469d27a3910be53e018e',
                           'X-AMZ-Date': '20220715T212200Z',
                           'x-amz-content-sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
                           'x-amz-security-token': 'YOUR_SESSION_TOKEN'}
        mock_request = Mock()
        mock_request.url = 'https://some-es.us-east-1.es.amazonaws.com:80/'
        mock_request.method = "GET"
        mock_request.body = None
        mock_request.headers = {}

        frozen_datetime = datetime.datetime(2022, 7, 15, 21, 22, 0)

        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = frozen_datetime
            self.auth(mock_request)

        self.assertEqual(expected_result, mock_request.headers)


    def test_get_signature_key(self):
        expected_result = (b'O\x91\xd3\x02\xc7\\J\xf7\xe0\x96[\xc6\xa8V\x81\n\x0c\x7f\xea\xe8R\xe6Pd'
                           b'\x94e[@]\x7f\x9e`')
        self.auth.datestamp = '20220715'

        result = self.auth.get_signature_key()
        self.assertEqual(result, expected_result)


    def test_get_headers_and_credential_date(self):
        expected_result = ('20220715T212200Z', '20220715')
        frozen_datetime = datetime.datetime(2022, 7, 15, 21, 22, 0)

        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = frozen_datetime

            result = self.auth.get_headers_and_credential_date()
            self.assertEqual(result, expected_result)


    def test_get_payload_hash__get_method(self):
        expected_result = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'

        mock_request = Mock()
        mock_request.url = 'https://some-es.us-east-1.es.amazonaws.com:80/'
        mock_request.method = "GET"
        mock_request.body = None

        result = self.auth.get_payload_hash(mock_request)

        self.assertEqual(result, expected_result)


    def test_get_payload_hash__post_method(self):
        expected_result = '3ba8907e7a252327488df390ed517c45b96dead033600219bdca7107d1d3f88a'

        mock_request = Mock()
        mock_request.url = 'https://some-es.us-east-1.es.amazonaws.com:80/'
        mock_request.method = "POST"
        mock_request.body = 'foo=bar'

        result = AWSSigV4RequestGenerator.get_payload_hash(mock_request)

        self.assertEqual(result, expected_result)


    def test_get_payload_hash__post_method_no_body(self):
        expected_result = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'

        mock_request = Mock()
        mock_request.url = 'https://some-es.us-east-1.es.amazonaws.com:80/'
        mock_request.method = "POST"
        mock_request.body = None

        result = AWSSigV4RequestGenerator.get_payload_hash(mock_request)

        self.assertEqual(result, expected_result)


    def test_get_canonical_uri(self):
        expected_result = '/'
        mock_parse_result = Mock(scheme='https',
                                 netloc='some-es.us-east-1.es.amazonaws.com:80',
                                 path='/',
                                 params='',
                                 query='',
                                 fragment='')

        result = self.auth.get_canonical_uri(mock_parse_result)
        self.assertEqual(result, expected_result)


    def test_get_canonical_uri__with_pah(self):
        expected_result = '/endpoint'
        mock_parse_result = Mock(scheme='https',
                                 netloc='some-es.us-east-1.es.amazonaws.com:80',
                                 path='/endpoint',
                                 params='',
                                 query='',
                                 fragment='')

        result = self.auth.get_canonical_uri(mock_parse_result)
        self.assertEqual(result, expected_result)


    def test_get_canonical_querystring(self):
        expected_result = 'foo=go&loo=po'
        mock_parse_result = Mock(scheme='https',
                                 netloc='some-es.us-east-1.es.amazonaws.com:80',
                                 path='/endpoint',
                                 params='',
                                 query='foo=go&loo=po',
                                 fragment='')

        result = self.auth.get_canonical_querystring(mock_parse_result)
        self.assertEqual(result, expected_result)


    def test_get_canonical_headers(self):
        expected_result = 'host:search-service.us-east-1.es.amazonaws.com\nx-amz-date:20220715T212200Z\n' \
                          'x-amz-security-token:YOUR_SESSION_TOKEN\n'

        self.auth.amzdate = '20220715T212200Z'
        mock_parse_result = Mock(scheme='https',
                                 netloc='some-es.us-east-1.es.amazonaws.com:80',
                                 path='/endpoint',
                                 params='',
                                 query='foo=go&loo=po',
                                 fragment='')

        result = self.auth.get_canonical_headers(mock_parse_result)
        self.assertEqual(result, expected_result)


    def test_get_authorization_header(self):
        expected_result = "AWS4-HMAC-SHA256 Credential=YOUR_KEY_ID/20220715/us-east-1/es/aws4_request, " \
                          "SignedHeaders=host;x-amz-date;x-amz-security-token, " \
                          "Signature=2b83a6c8b8932c469125f668c8bf56d187d47411f06316f9656cb254aede0538"
        self.auth.datestamp = '20220715'
        self.auth.amzdate = '20220715T212200Z'

        signed_headers = 'host;x-amz-date;x-amz-security-token'
        canonical_request = '''GET
                            /
                            
                            host:search-service.us-east-1.es.amazonaws.com
                            x-amz-date:20220715T212200Z
                            x-amz-security-token:YOUR_SESSION_TOKEN
                            
                            host;x-amz-date;x-amz-security-token
                            e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'''

        result = self.auth.get_authorization_header(canonical_request, signed_headers)
        self.assertEqual(result, expected_result)


if __name__ == '__main__':
    unittest.main()
