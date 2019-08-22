import boto3
import datetime
import json
import logging
import os
import random
import re
import subprocess
import time
import unittest
import types

from copy import deepcopy
from pathlib import Path
import pprint
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch

from sosw.scheduler import Scheduler, InvalidJob, global_vars
from sosw.labourer import Labourer
from sosw.components.helpers import chunks
from sosw.test.variables import TEST_SCHEDULER_CONFIG
from sosw.test.helpers_test import line_count

import sosw.scheduler as module


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Scheduler_UnitTestCase(unittest.TestCase):
    TEST_CONFIG = TEST_SCHEDULER_CONFIG
    LABOURER = Labourer(id='some_function', arn='arn:aws:lambda:us-west-2:000000000000:function:some_function')
    FNAME = '/tmp/aglaya.txt'
    TODAY = datetime.date(2019, 1, 31)

    # Warning! Tthis Payload is not operational as it is. Should add `isolate_SOMETHING` in several places.
    PAYLOAD = {
        'sections': {
            'section_funerals':    {
                'stores': {
                    'store_flowers': None,
                    'store_caskets': None,
                },
            },
            'section_weddings':    {
                'stores': {
                    'store_flowers': None,
                    'store_limos':   None,
                    'store_music':   {
                        'products': ['product_march', 'product_chorus', 740, 'product,4', 'product 5'],
                    },
                }
            },
            'section_conversions': {
                'stores': {
                    'store_training':     {
                        'products': {
                            'product_history': None,
                            'product_prayer':  None,
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

        self.custom_config = deepcopy(self.TEST_CONFIG)
        self.custom_config['siblings_config'] = {
            'auto_spawning': True
        }

        lambda_context = types.SimpleNamespace()
        lambda_context.aws_request_id = 'AWS_REQ_ID'
        lambda_context.invoked_function_arn = 'arn:aws:lambda:us-west-2:000000000000:function:some_function'
        lambda_context.get_remaining_time_in_millis = MagicMock(return_value=300000)  # 5 minutes
        global_vars.lambda_context = lambda_context
        self.custom_lambda_context = global_vars.lambda_context  # This is to access from tests.

        with patch('boto3.client'):
            self.scheduler = module.Scheduler(self.custom_config)

        self.scheduler.s3_client = MagicMock()
        self.scheduler.sns_client = MagicMock()
        self.scheduler.task_client = MagicMock()
        self.scheduler.task_client.get_labourer.return_value = self.LABOURER
        self.scheduler.siblings_client = MagicMock()

        self.scheduler.st_time = time.time()


    def tearDown(self):
        self.patcher.stop()

        try:
            del (os.environ['AWS_LAMBDA_FUNCTION_NAME'])
        except Exception:
            pass

        for fname in [self.scheduler.local_queue_file, self.FNAME]:
            try:
                os.remove(fname)
            except Exception:
                pass


    def put_local_file(self, file_name=None, json=False):
        with open(file_name or self.scheduler.local_queue_file, 'w') as f:
            for x in range(10):
                if json:
                    f.write('{"key": "val", "number": "42", "boolean": true, "labourer_id": "some_function"}\n')
                else:
                    f.write(f"Hello Aglaya {x} {random.randint(0, 99)}\n")


    @staticmethod
    def line_count(file):
        return int(subprocess.check_output('wc -l {}'.format(file), shell=True).split()[0])


    def test_init__chunkable_attrs_not_end_with_s(self):
        config = self.custom_config
        config['job_schema']['chunkable_attrs'] = [('bad_name_ending_with_s', {})]

        with patch('boto3.client'):
            self.assertRaises(AssertionError, Scheduler, custom_config=config)


    def test_get_next_chunkable_attr(self):
        self.assertEqual(self.scheduler.get_next_chunkable_attr('store'), 'product')
        self.assertEqual(self.scheduler.get_next_chunkable_attr('stores'), 'product')
        self.assertEqual(self.scheduler.get_next_chunkable_attr('section'), 'store')
        self.assertIsNone(self.scheduler.get_next_chunkable_attr('product'))
        self.assertIsNone(self.scheduler.get_next_chunkable_attr('bad_name'))


    def test__queue_bucket(self):
        self.assertEqual(self.scheduler._queue_bucket, self.scheduler.config['queue_bucket'])


    def test__remote_queue_file(self):
        self.assertIn(f"{self.scheduler.config['s3_prefix'].strip('/')}", self.scheduler.remote_queue_file)
        self.assertIn(self.custom_lambda_context.aws_request_id, self.scheduler.remote_queue_file)


    def test__remote_queue_locked_file(self):
        self.assertIn(f"{self.scheduler.config['s3_prefix'].strip('/')}", self.scheduler.remote_queue_locked_file)
        self.assertIn('locked_', self.scheduler.remote_queue_locked_file)
        self.assertIn(self.custom_lambda_context.aws_request_id, self.scheduler.remote_queue_locked_file)


    ### Tests of file operations ###
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
        self.assertFalse(os.path.isfile(self.FNAME))
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

        self.assertFalse(os.path.isfile(self.FNAME))


    def test_process_file(self):
        self.put_local_file(self.FNAME, json=True)
        self.scheduler.get_and_lock_queue_file = MagicMock(return_value=self.FNAME)
        self.scheduler.upload_and_unlock_queue_file = MagicMock()
        self.scheduler.task_client = MagicMock()
        self.scheduler.clean_tmp = MagicMock()

        # This is a specific test patch for logging of remaining time.
        # We actually want two rounds: first OK, second - low time. But the context.method is called twice each round.
        self.custom_lambda_context.get_remaining_time_in_millis.side_effect = [300000, 300000, 1000, 1000]

        with patch('sosw.scheduler.Scheduler._sleeptime_for_dynamo', new_callable=PropertyMock) as mock_sleeptime:
            mock_sleeptime.return_value = 0.0001

            self.scheduler.process_file()

            self.assertEqual(self.scheduler.task_client.create_task.call_count, 10)
            self.assertEqual(mock_sleeptime.call_count, 10)

            self.scheduler.upload_and_unlock_queue_file.assert_called_once()
            self.scheduler.clean_tmp.assert_called_once()
            # number of calls depends on the 'remaining_time_in_millis()' mock
            self.assertEqual(self.scheduler.siblings_client.spawn_sibling.call_count, 1)


    ### Tests of construct_job_data ###
    def test_construct_job_data(self):

        self.scheduler.chunk_dates = MagicMock(return_value=[{'a': 'foo'}, {'b': 'bar'}])
        self.scheduler.chunk_job = MagicMock()

        r = self.scheduler.construct_job_data({'pl': 1})

        self.scheduler.chunk_dates.assert_called_once()
        self.scheduler.chunk_job.assert_called()
        self.assertEqual(self.scheduler.chunk_job.call_count, 2)


    def test_construct_job_data__preserve_skeleton_through_chunkers(self):

        r = self.scheduler.construct_job_data({'pl': 1}, skeleton={'labourer_id': 'some'})
        print(r)

        for task in r:
            self.assertEqual(task['labourer_id'], 'some')


    def test_construct_job_data__empty_job(self):

        JOB = dict()
        r = self.scheduler.construct_job_data(JOB)
        self.assertEqual(r, [JOB])


    def test_construct_job_data__real_payload__for_debuging_logs(self):
        JOB = {
            'lambda_name':         'some_lambda',
            'period':              'last_2_days', 'isolate_days': True,
            'sections':            {
                '111': {'all_campaigns': True},
                '222': {'all_campaigns': True},
                '333': {
                    'isolate_stores': True,
                    'all_campaigns':  False,
                    'stores':         {'333-111': None, '333-222': None, '333-333': {'keep_me': 7}},
                }
            }, 'isolate_sections': 'True'
        }

        r = self.scheduler.construct_job_data(JOB)

        for t in r:
            print(t)

        self.assertEqual(len(r), 10)
        # self.assertEqual(1, 42)


    ### Tests of chunk_dates ###
    def test_chunk_dates(self):
        TESTS = [
            ({'period': 'today'}, 'today'),
            ({'period': 'yesterday'}, 'yesterday'),
            ({'period': 'last_3_days'}, 'last_x_days'),
            ({'period': '10_days_back'}, 'x_days_back'),
            ({'period': 'previous_2_days'}, 'previous_x_days'),
            ({'period': 'last_week'}, 'last_week')
        ]

        for test, func_name in TESTS:
            FUNCTIONS = ['today', 'yesterday', 'last_x_days', 'x_days_back', 'previous_x_days', 'last_week']
            for f in FUNCTIONS:
                setattr(self.scheduler, f, MagicMock())

            self.scheduler.chunk_dates(test)

            func = getattr(self.scheduler, func_name)
            func.assert_called_once()

            for bad_f_name in [x for x in FUNCTIONS if not x == func_name]:
                bad_f = getattr(self.scheduler, bad_f_name)
                bad_f.assert_not_called()


    def test_chunk_dates__preserve_skeleton(self):
        TESTS = [
            {'period': 'last_1_days', 'a': 'foo'},
            {'period': 'last_10_days', 'a': 'foo'},
            {'period': 'last_10_days', 'isolate_days': True, 'a': 'foo'},
            {'period': '1_days_back', 'a': 'foo'},
            {'period': '10_days_back', 'a': 'foo'},
            {'period': '10_days_back', 'isolate_days': True, 'a': 'foo'},  # Isolation here is abundant
        ]

        SKELETON = {'labourer_id': 'some'}

        for test in TESTS:
            if test.get('isolate_days'):
                pattern = '[a-z]+_([0-9]+)_days'
                try:
                    expected_number = int(re.match(pattern, test['period'])[1])
                except Exception:
                    expected_number = 1
            else:
                expected_number = 1

            r = self.scheduler.chunk_dates(job=test, skeleton=SKELETON)
            self.assertEqual(len(r), expected_number)
            for task in r:
                self.assertEqual(task['labourer_id'], 'some')


    def test_chunk_dates__preserve_skeleton__if_no_chunking(self):
        TASK = {
            'a': 'foo'
        }
        SKELETON = {'labourer_id': 'some'}

        r = self.scheduler.chunk_dates(job=TASK, skeleton=SKELETON)

        for task in r:
            self.assertEqual(task['labourer_id'], 'some')
            self.assertEqual(task['a'], 'foo')


    def test_chunk_dates__pops_period(self):
        TASK = {
            'period': 'last_42_days',
            'a':      'foo'
        }

        r = self.scheduler.chunk_dates(job=TASK)

        self.assertIn('period', TASK, "DANGER! Modified initial job!")
        for task in r:
            self.assertNotIn('period', task)
            self.assertEqual(task['a'], 'foo')


    def test_chunk_dates__last_x_days(self):

        TASK = {'period': 'last_5_days', 'isolate_days': True, 'a': 'foo'}
        today = datetime.date(2019, 1, 30)

        with patch('sosw.scheduler.datetime.date') as mdt:
            mdt.today.return_value = today
            r = self.scheduler.chunk_dates(TASK)

        self.assertEqual(len(r), 5)
        for i, task in enumerate(r):
            self.assertEqual(task['a'], 'foo')
            self.assertEqual(task['date_list'], [f"{self.TODAY - datetime.timedelta(days=6-i)}"])


    def test_chunk_dates__raises_invalid_period_pattern(self):
        TASK = {'period': 'putin_the_best'}
        self.assertRaises(ValueError, self.scheduler.chunk_dates, job=TASK), "Putin is not supported"


    def test_last_x_days(self):

        TESTS = [
            ('last_3_days', ['2019-01-27', '2019-01-28', '2019-01-29']),
            ('last_5_days', ['2019-01-25', '2019-01-26', '2019-01-27', '2019-01-28', '2019-01-29']),
        ]
        today = datetime.date(2019, 1, 30)

        with patch('sosw.scheduler.datetime.date') as mdt:
            mdt.today.return_value = today

            for test, expected in TESTS:
                self.assertEqual(self.scheduler.last_x_days(test), expected)


    def test_x_days_back(self):

        TESTS = [
            ('1_days_back', ['2019-01-29']),
            ('7_days_back', ['2019-01-23']),
            ('30_days_back', ['2018-12-31']),
        ]
        today = datetime.date(2019, 1, 30)

        with patch('sosw.scheduler.datetime.date') as mdt:
            mdt.today.return_value = today

            for test, expected in TESTS:
                self.assertEqual(self.scheduler.x_days_back(test), expected)

            last_week = self.scheduler.x_days_back('7_days_back')[0]
        self.assertEqual(today.weekday(), datetime.datetime.strptime(last_week, '%Y-%m-%d').weekday())


    def test_yesterday(self):

        TESTS = [
            ('yesterday', ['2019-04-10']),
        ]

        today = datetime.date(2019, 4, 11)

        with patch('sosw.scheduler.datetime.date') as mdt:
            mdt.today.return_value = today

            for test, expected in TESTS:
                self.assertEqual(self.scheduler.yesterday(test), expected)

    def test_today(self):
        TESTS = [
            ('today', ['2019-04-10']),
        ]
        today = datetime.date(2019, 4, 10)

        with patch('sosw.scheduler.datetime.date') as mdt:
            mdt.today.return_value = today

            for test, expected in TESTS:
                self.assertEqual(self.scheduler.today(test), expected)

    def test_previous_x_days(self):
        today = datetime.date(2019, 4, 30)

        TESTS = [
            ('previous_2_days', ['2019-04-26', '2019-04-27']),
            ('previous_3_days', ['2019-04-24', '2019-04-25', '2019-04-26'])
        ]

        with patch('sosw.scheduler.datetime.date') as mdt:
            mdt.today.return_value = today

            for test, expected in TESTS:
                self.assertEqual(self.scheduler.previous_x_days(test), expected)

    def test_last_week(self):
        today = datetime.date(2019, 4, 30)

        TESTS = [
            ('last_week', ['2019-04-21',
                           '2019-04-22',
                           '2019-04-23',
                           '2019-04-24',
                           '2019-04-25',
                           '2019-04-26',
                           '2019-04-27'])
        ]

        with patch('sosw.scheduler.datetime.date') as mdt:
            mdt.today.return_value = today

            for test, expected in TESTS:
                self.assertEqual(self.scheduler.last_week(test), expected)


    ### Tests of chunk_job ###
    def test_chunk_job__not_chunkable_config(self):
        self.scheduler.chunkable_attrs = []
        pl = deepcopy(self.PAYLOAD)

        r = self.scheduler.chunk_job(job=pl)
        # pprint.pprint(r)
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0], pl)


    def test_chunk_job__not_raises_unchunkable_subtask__but_preserves_in_payload(self):
        pl = deepcopy(self.PAYLOAD)
        pl['sections']['section_conversions']['stores']['store_training']['isolate_products'] = True
        pl['sections']['section_conversions']['stores']['store_training']['products']['product_book'] = {
            'product_versions':
                {
                    'product_version_audio': None,
                    'product_version_paper': None,
                }
        }


        def find_product(t):
            try:
                return set(t['product_versions'].keys()) == {'product_version_audio', 'product_version_paper'}
            except Exception:
                return False


        # print(pl)
        r = self.scheduler.chunk_job(job=pl)
        # for t in r:
        #     print(t)

        self.assertTrue(any(find_product(task) for task in r))


    def test_chunk_job__raises__unsupported_vals__string(self):
        pl = deepcopy(self.PAYLOAD)

        pl['sections']['section_conversions']['isolate_stores'] = True
        pl['sections']['section_conversions']['stores']['store_training'] = 'some_string'

        self.assertRaises(InvalidJob, self.scheduler.chunk_job, job=pl)


    def test_chunk_job__raises__unsupported_vals__list_not_as_value(self):
        pl = deepcopy(self.PAYLOAD)
        pl['sections']['section_conversions']['isolate_stores'] = True
        pl['sections']['section_conversions']['stores']['store_training'] = ['just_a_string']

        self.assertRaises(InvalidJob, self.scheduler.chunk_job, job=pl)


    def test_chunk_job__not_raises__notchunkable__if_no_isolation(self):
        pl = deepcopy(self.PAYLOAD)

        pl['isolate_sections'] = True
        pl['sections']['section_conversions']['stores']['store_training'] = 'some_string'

        r = self.scheduler.chunk_job(job=pl)
        val = r[2]
        print(r)
        print(f"We chunked only first level (sections). The currently interesting is section #3, "
              f"where we put custom unchunkable payload: {val}")

        self.assertEqual(val['stores']['store_training'], 'some_string')


    def test_chunk_job(self):

        pl = deepcopy(self.PAYLOAD)
        pl['sections']['section_weddings']['stores']['store_music']['isolate_products'] = True
        pl['sections']['section_conversions']['stores']['store_training']['isolate_products'] = True

        response = self.scheduler.chunk_job(job=pl)

        # for row in response:
        #     pprint.pprint(row)
        #     print('\n')

        NUMBER_TASKS_EXPECTED = [
            ('sections', 'section_funerals', 1),
            ('sections', 'section_weddings', 7),
            ('sections', 'section_conversions', 4),
            ('stores', 'store_training', 2),
            ('stores', 'store_baptizing', 1),
            ('sections', 'section_gifts', 1),
        ]

        self.check_number_of_tasks(NUMBER_TASKS_EXPECTED, response)


    def test_chunk_job__unchunckable_preserve_custom_attrs(self):

        pl = {
            'sections': {
                'section_funerals': {'custom': 'data'},
                'section_weddings': None,
            }
        }

        response = self.scheduler.chunk_job(job=pl)
        # print(response)

        self.assertEqual([pl], response)


    def test_chunk_job__max_items_per_batch(self):
        """
        Tests that `max_products_per_batch` will actually make chunks of products of specific size.

        Here we have a tricky case:

        `section_weddings` has 3 different `stores`. In `store_music` we have 5 `products`.
        With max_products_per_batch we should have:

        - store_1
        - store_2
        - store_3, products 1 + 2
        - store_3, products 3 + 4
        - store_3, products 5
        """
        pl = deepcopy(self.PAYLOAD)
        pl['sections']['section_weddings']['stores']['store_music']['max_products_per_batch'] = 2

        response = self.scheduler.chunk_job(job=pl)

        NUMBER_TASKS_EXPECTED = [
            ('sections', 'section_weddings', 5),
        ]

        # for row in response:
        #     pprint.pprint(row)
        #     print('\n')

        self.check_number_of_tasks(NUMBER_TASKS_EXPECTED, response)
        batches = [x['products'] for x in response if x.get('stores') == ['store_music']]
        print(batches)

        self.assertEqual(batches,
                         list(chunks(pl['sections']['section_weddings']['stores']['store_music']['products'], 2)))


    ### Tests of other methods ###
    def test_extract_job_from_payload(self):

        TESTS = [
            ({'job': {'lambda_name': 'foo', 'payload_attr': 'val'}}, {'lambda_name': 'foo', 'payload_attr': 'val'}),
            ({'lambda_name': 'foo', 'payload_attr': 'val'}, {'lambda_name': 'foo', 'payload_attr': 'val'}),
            ({'lambda_name': 'arn:aws:lambda:us-west-2:000000000000:function:foo', 'payload_attr': 'val'},
             {'lambda_name': 'foo', 'payload_attr': 'val'}),
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


    def test_needs_chunking__max_items_per_batch(self):

        pl = deepcopy(self.PAYLOAD)

        # Verify that no chunking is required by default
        self.assertFalse(self.scheduler.needs_chunking('sections', pl))

        # Inject max_items_per_batch and recheck.
        pl['sections']['section_conversions']['stores']['store_training']['max_products_per_batch'] = 3
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


    def check_number_of_tasks(self, expected_map, response):
        for key, val, expected in expected_map:
            r = filter(lambda task: task.get(key) == [val], response)
            # print(f"TEST OF FILTER: {t}: {len(list(t))}")
            self.assertEqual(len(list(r)), expected)


    def test_validate_list_of_vals(self):
        TESTS = [
            ({'a': None, 'b': None}, ['a', 'b']),
            (['a', 'b', 42], ['a', 'b', 42]),
            ([], []),
        ]

        for test, expected in TESTS:
            self.assertEqual(self.scheduler.validate_list_of_vals(test), expected)


    def test_get_and_lock_queue_file__s3_calls(self):

        self.scheduler.get_and_lock_queue_file()
        self.scheduler.s3_client.download_file.assert_called_once()
        self.scheduler.s3_client.copy_object.assert_called_once()
        self.scheduler.s3_client.delete_object.assert_called_once()
        self.scheduler.s3_client.upload_file.assert_not_called()


    def test_get_and_lock_queue_file__local_file_exists(self):

        with patch('os.path.isfile') as isfile_mock:
            isfile_mock.return_value = True

            r = self.scheduler.get_and_lock_queue_file()

        self.assertEqual(r, self.scheduler.local_queue_file)
        self.scheduler.s3_client.download_file.assert_not_called()
        self.scheduler.s3_client.copy_object.assert_not_called()
        self.scheduler.s3_client.delete_object.assert_not_called()

        self.scheduler.s3_client.upload_file.assert_called_once()


    def test_parse_job_to_file(self):

        SAMPLE_SIMPLE_JOB = {
            'lambda_name':  self.LABOURER.id,
            'some_payload': 'foo',
        }

        self.scheduler.parse_job_to_file(SAMPLE_SIMPLE_JOB)

        self.assertEqual(line_count(self.scheduler.local_queue_file), 1)

        with open(self.scheduler.local_queue_file, 'r') as f:
            row = json.loads(f.read())
            print(row)

            self.assertEqual(row['labourer_id'], self.LABOURER.id)
            self.assertEqual(row['some_payload'], 'foo')


    def test_parse_job_to_file__multiple_rows(self):

        SAMPLE_SIMPLE_JOB = {
            'lambda_name':      self.LABOURER.id,
            "isolate_sections": True,
            'sections':         {
                'section_technic':   None,
                'section_furniture': None,
            },
        }

        self.scheduler.parse_job_to_file(SAMPLE_SIMPLE_JOB)

        self.assertEqual(line_count(self.scheduler.local_queue_file), 2)

        with open(self.scheduler.local_queue_file, 'r') as f:
            for row in f.readlines():
                # print(row)
                parsed_row = json.loads(row)
                print(parsed_row)

                self.assertEqual(parsed_row['labourer_id'], self.LABOURER.id)
                self.assertEqual(len(parsed_row['sections']), 1)
                self.assertIn(parsed_row['sections'][0], SAMPLE_SIMPLE_JOB['sections'])


    def test_call__sample(self):
        SAMPLE_SIMPLE_JOB = {
            'lambda_name':  self.LABOURER.id,
            'some_payload': 'foo',
        }

        print(json.dumps(SAMPLE_SIMPLE_JOB))
        r = self.scheduler(json.dumps(SAMPLE_SIMPLE_JOB))
        print(r)

        self.scheduler.task_client.create_task.assert_called_once()

        self.scheduler.s3_client.download_file.assert_not_called()
        self.scheduler.s3_client.copy_object.assert_not_called()

        self.scheduler.s3_client.upload_file.assert_called_once()
        self.scheduler.s3_client.delete_object.assert_called_once()

