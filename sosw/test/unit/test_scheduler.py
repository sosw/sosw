import boto3
import logging
import os
import random
import subprocess
import time
import unittest

from copy import deepcopy
from pathlib import Path
import pprint
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch

from sosw.scheduler import Scheduler
from sosw.labourer import Labourer
from sosw.test.variables import TEST_SCHEDULER_CONFIG


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Scheduler_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_SCHEDULER_CONFIG
    FNAME = '/tmp/aglaya.txt'

    PAYLOAD = {
        'sections': {
            'section_funerals':    {
                'stores': {
                    'store_flowers': None,
                    'store_caskets': None,
                }
            },
            'section_conversions': {
                'stores': {
                    'store_training':     {
                        'products': {
                            'product_history': None,
                            'product_prayer':  None,
                            'product_books':   {
                                'product_versions':
                                    {
                                        'product_version_audio': None,
                                        'product_version_paper': None,
                                    }
                            }
                        }
                    },
                    'store_baptizing':    None,
                    'store_circumcision': None
                }
            },
            'section_gifts':       None
        }
    }


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.custom_config = self.TEST_CONFIG.copy()
        self.scheduler = Scheduler(self.custom_config)
        self.scheduler.st_time = time.time()


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except:
            pass

        try:
            os.remove(self.FNAME)
        except:
            pass


    def put_local_file(self, file_name=None, json=False):
        with open(file_name or self.scheduler._local_queue_file, 'w') as f:
            for x in range(10):
                if json:
                    f.write('{"key": "val", "number": "42", "boolean": true}\n')
                else:
                    f.write(f"Hello Aglaya {x} {random.randint(0, 99)}\n")


    @staticmethod
    def line_count(file):
        return int(subprocess.check_output('wc -l {}'.format(file), shell=True).split()[0])


    def test__queue_bucket(self):
        self.assertEqual(self.scheduler._queue_bucket, self.scheduler.config['queue_bucket'])


    def test__local_queue_file(self):
        self.assertEqual(self.scheduler._local_queue_file, f"/tmp/{self.scheduler.config['queue_file']}")


    def test__remote_queue_file(self):
        self.assertEqual(self.scheduler._remote_queue_file,
                         f"{self.scheduler.config['s3_prefix'].strip('/')}/"
                         f"{self.scheduler.config['queue_file'].strip('/')}")


    def test__remote_queue_locked_file(self):
        self.assertEqual(self.scheduler._remote_queue_locked_file,
                         f"{self.scheduler.config['s3_prefix'].strip('/')}/locked_"
                         f"{self.scheduler.config['queue_file'].strip('/')}")


    def test_pop_rows_from_file(self):
        self.put_local_file(self.FNAME)

        # Initial setup made 10 rows.
        self.assertEqual(self.line_count(self.FNAME), 10)

        # Pop a single top row
        self.scheduler.pop_rows_from_file(self.FNAME)
        self.assertEqual(self.line_count(self.FNAME), 9)

        # Pop multiple rows
        self.scheduler.pop_rows_from_file(self.FNAME, rows=5)
        self.assertEqual(self.line_count(self.FNAME), 4)

        # Catch StopIteration and return only remaining.
        r = self.scheduler.pop_rows_from_file(self.FNAME, rows=42)
        self.assertEqual(self.line_count(self.FNAME), 0)
        self.assertEqual(len(r), 4)


    def test_pop_rows_from_file__reads_from_top(self):
        self.put_local_file(self.FNAME)

        r = self.scheduler.pop_rows_from_file(self.FNAME, rows=9)

        self.assertEqual(len(r), 9)
        self.assertTrue(r[0].startswith('Hello Aglaya 0'))

        with open(self.FNAME) as f:
            row = f.read()
            self.assertTrue(row.startswith('Hello Aglaya 9'))


    def test_pop_rows_from_file__missing_or_empty_file(self):
        # Missing file
        self.assertEqual(self.scheduler.pop_rows_from_file(self.FNAME), list())

        # Empty file
        Path(self.FNAME).touch()
        self.assertEqual(self.scheduler.pop_rows_from_file(self.FNAME), list())


    def test_process_file(self):
        self.put_local_file(self.FNAME, json=True)
        self.scheduler.get_and_lock_queue_file = MagicMock(return_value=self.FNAME)
        self.scheduler.upload_and_unlock_queue_file = MagicMock()
        self.scheduler.task_client = MagicMock()

        with patch('sosw.scheduler.Scheduler._sleeptime_for_dynamo', new_callable=PropertyMock) as mock_sleeptime:
            mock_sleeptime.return_value = 0.0001

            self.scheduler.process_file()

            self.assertEqual(self.scheduler.task_client.create_task.call_count, 10)
            self.assertEqual(mock_sleeptime.call_count, 10)

            self.scheduler.upload_and_unlock_queue_file.assert_called_once()


    def test_extract_job_from_payload(self):

        TESTS = [
            ({'job': {'lambda_name': 'foo', 'payload_attr': 'val'}}, {'lambda_name': 'foo', 'payload_attr': 'val'}),
            ({'lambda_name': 'foo', 'payload_attr': 'val'}, {'lambda_name': 'foo', 'payload_attr': 'val'}),
            ({'job': {'lambda_name': 'foo', 'payload_attr': 'val'}}, {'lambda_name': 'foo', 'payload_attr': 'val'}),

            # JSONs
            ('{"lambda_name": "foo", "payload_attr": "val"}', {'lambda_name': 'foo', 'payload_attr': 'val'}),
            ('{"job": {"lambda_name": "foo", "payload_attr": "val"}}', {'lambda_name': 'foo', 'payload_attr': 'val'}),
            ('{"job": "{\\"lambda_name\\": \\"foo\\", \\"payload_attr\\": \\"val\\"}"}',
             {'lambda_name': 'foo', 'payload_attr': 'val'}),

        ]

        for test, expected in TESTS:
            self.assertEqual(self.scheduler.extract_job_from_payload(test), expected)


    def test_extract_job_from_payload_raises(self):

        TESTS = [
            42,
            {'payload_attr': 'val'},
            "{'payload_attr': 'val'}",
            {'job': {'payload_attr': 'val'}},
            {"job": "bad one"},
        ]

        for test in TESTS:
            self.assertRaises(Exception, self.scheduler.extract_job_from_payload, test)


    def test_get_name_from_arn(self):

        TESTS = [
            ('bar_with_no_arn', 'bar_with_no_arn'),
            ('arn:aws:lambda:us-west-2:000000000000:function:bar', 'bar'),
            ('arn:aws:lambda:us-west-2:000000000000:function:bar:', 'bar'),
            ('arn:aws:lambda:us-west-2:000000000000:function:bar:$LATEST', 'bar'),
            ('arn:aws:lambda:us-west-2:000000000000:function:bar:12', 'bar'),
        ]

        for test, expected in TESTS:
            self.assertEqual(self.scheduler.get_name_from_arn(test), expected)


    def test_needs_chunking__isolate_root(self):

        pl = deepcopy(self.PAYLOAD)
        self.assertFalse(self.scheduler.needs_chunking('sections', pl))

        pl = deepcopy(self.PAYLOAD)
        pl['isolate_sections'] = True
        self.assertTrue(self.scheduler.needs_chunking('sections', pl))


    def test_needs_chunking__isolate_subdata(self):

        pl = deepcopy(self.PAYLOAD)
        pl['sections']['section_funerals']['isolate_stores'] = True

        self.assertTrue(self.scheduler.needs_chunking('sections', pl))
        self.assertTrue(self.scheduler.needs_chunking('stores', pl['sections']['section_funerals']))
        self.assertFalse(self.scheduler.needs_chunking('stores', pl['sections']['section_conversions']))


    def test_needs_chunking__isolate_subdata_deep(self):

        pl = deepcopy(self.PAYLOAD)
        pl['sections']['section_conversions']['stores']['store_training']['isolate_products'] = True
        # pprint.pprint(pl)

        self.assertFalse(self.scheduler.needs_chunking('stores', pl['sections']['section_funerals']))
        self.assertTrue(self.scheduler.needs_chunking('stores', pl['sections']['section_conversions']))
        self.assertTrue(self.scheduler.needs_chunking(
                'products', pl['sections']['section_conversions']['stores']['store_training']))

        self.assertTrue(self.scheduler.needs_chunking('sections', pl))


    def test_get_index_from_list(self):

        TESTS = [
            (0, 'a', ['a', 'b', 'c']),
            (0, 'name', ['names', 'b', 'c']),
            (2, 'c', ['a', 'b', 'c']),
            (1, 'b', {'a': 1, 'b': 2, 'c': 3}),
            (1, 'bob', {'a': 1, 'bobs': 2, 'c': 3}),
        ]

        for expected, attr, data in TESTS:
            self.assertEqual(expected, self.scheduler.get_index_from_list(attr, data))
