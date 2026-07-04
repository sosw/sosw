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
from sosw.managers.meta_handler import MetaHandler
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
        self.get_config_patch.return_value = {}
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
            self.scheduler = module.Scheduler(custom_config=self.custom_config)

        self.scheduler.s3_client = MagicMock()
        self.scheduler.sns_client = MagicMock()
        self.scheduler.task_client = MagicMock()
        self.scheduler.task_client.get_labourer.return_value = self.LABOURER
        self.scheduler.get_db_field_name = lambda key: key
        self.scheduler.siblings_client = MagicMock()
        self.scheduler.meta_handler = MagicMock(signature=MetaHandler)

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


    def test_process_file__no_file_in_queue(self):
        self.scheduler.get_and_lock_queue_file = MagicMock(return_value=None)
        self.scheduler.upload_and_unlock_queue_file = MagicMock()
        self.scheduler.clean_tmp = MagicMock()

        self.scheduler.process_file()

        self.scheduler.upload_and_unlock_queue_file.assert_not_called()
        self.scheduler.clean_tmp.assert_not_called()


    def test_process_file__spawn_sibling_failure_is_not_fatal(self):
        """
        If spawning a sibling fails, the file must still be uploaded back and the tmp cleaned.
        """

        self.put_local_file(self.FNAME, json=True)
        self.scheduler.get_and_lock_queue_file = MagicMock(return_value=self.FNAME)
        self.scheduler.upload_and_unlock_queue_file = MagicMock()
        self.scheduler.clean_tmp = MagicMock()
        self.scheduler.siblings_client.spawn_sibling.side_effect = Exception("Failed to invoke sibling")

        # Not enough remaining time from the very beginning: go straight to spawning a sibling.
        self.custom_lambda_context.get_remaining_time_in_millis = MagicMock(return_value=1000)

        self.scheduler.process_file()

        self.scheduler.siblings_client.spawn_sibling.assert_called_once()
        self.assertNotIn('siblings_spawned', self.scheduler.stats)
        self.scheduler.upload_and_unlock_queue_file.assert_called_once()
        self.scheduler.clean_tmp.assert_called_once()


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


    def test_chunk_dates__custom_period_patterns__raises_non_string_pattern(self):
        self.scheduler.config['custom_period_patterns'] = ['good_pattern', 42]

        with self.assertRaises(TypeError) as exc:
            self.scheduler.chunk_dates(job={'period': 'today'})

        self.assertIn("expected to be str", str(exc.exception))


    def test_chunk_dates__custom_period_patterns__raises_not_a_list(self):
        self.scheduler.config['custom_period_patterns'] = 'just_a_string'

        with self.assertRaises(TypeError) as exc:
            self.scheduler.chunk_dates(job={'period': 'today'})

        self.assertIn("expected to be (list, tuple)", str(exc.exception))


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


    def test_custom_period_patterns(self):

        class ChildScheduler(module.Scheduler):

            def __init__(self, custom_config):
                super().__init__(custom_config=custom_config)

            def get_june_days(self):
                return ['2020-06-24', '2020-06-23', '2020-06-22']

        with patch('boto3.client'):
            custom_config = deepcopy(self.TEST_CONFIG)
            custom_config['custom_period_patterns'] = ['get_june_days']
            child = ChildScheduler(custom_config=custom_config)

            r = child.chunk_dates(job={'period': 'get_june_days'})

            self.assertEqual(r, [{'date_list': ['2020-06-24', '2020-06-23', '2020-06-22']}])


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


    def test_get_isolate_attributes_from_job(self):

        GOOD = {'isolate_sections': True, 'isolate_Ss': False, 'max_stores_cool_per_batch': 42}
        BAD = {'sections': True, 'foo': {'baz': 17}}

        result = self.scheduler.get_isolate_attributes_from_job(data={**GOOD, **BAD})
        self.assertDictEqual(result, GOOD)


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
        For example with max_products_per_batch = 2, we should have:

        - store_1
        - store_2
        - store_3, products 1 + 2
        - store_3, products 3 + 4
        - store_3, products 5
        """
        pl = deepcopy(self.PAYLOAD)
        pl['sections']['section_weddings']['stores']['store_music']['max_products_per_batch'] = 2
        # pl['sections']['section_funerals']['isolate_stores'] = True
        # pl['isolate_sections'] = True
        # pl['isolate_stores'] = True

        response = self.scheduler.chunk_job(job=pl)

        NUMBER_TASKS_EXPECTED = [
            ('sections', 'section_weddings', 5),
            ('sections', 'section_funerals', 1),
            ('sections', 'section_conversions', 1),
            ('sections', 'section_gifts', 1),
        ]

        # for row in response:
        #     pprint.pprint(row)
        #     print('\n')

        self.check_number_of_tasks(NUMBER_TASKS_EXPECTED, response)
        batches = [x['products'] for x in response if x.get('stores') == ['store_music']]
        print(batches)

        self.assertEqual(batches,
                         list(chunks(pl['sections']['section_weddings']['stores']['store_music']['products'], 2)))
        # self.assertEqual(1, 2)


    def test_chunk_job__root_level_isolate(self):
        """
        Tests that `isolate_ATTRs` in the root of the Payload will be respected for chunking in all the nested
        elements of the Job recursively.

        In the test payload the 'stores' is the second level of nesting attribute. But we pass it in the root of job.
        """
        pl = deepcopy(self.PAYLOAD)
        pl['isolate_stores'] = True

        response = self.scheduler.chunk_job(job=pl)

        NUMBER_TASKS_EXPECTED = [
            ('sections', 'section_funerals', 2),
            ('sections', 'section_weddings', 3),
            ('sections', 'section_conversions', 3),
            ('sections', 'section_gifts', 1),
        ]

        # for row in response:
        #     pprint.pprint(row)
        #     print('\n')

        self.check_number_of_tasks(NUMBER_TASKS_EXPECTED, response)


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


    def test_validate_list_of_vals__single_key_dict_with_embedded_data(self):
        DATA = {'a': {'nested': 'payload'}}
        self.assertEqual(self.scheduler.validate_list_of_vals(DATA), [DATA])


    def test_validate_list_of_vals__raises(self):
        TESTS = [
            ['flat', {'embedded': 'dict'}],  # List with values of unsupported types.
            {'a': {'x': 1}, 'b': None},  # Multi-key dict with embedded data.
            42,  # Not an iterable at all.
        ]

        for test in TESTS:
            with self.subTest(test=test):
                self.assertRaises(InvalidJob, self.scheduler.validate_list_of_vals, test)


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


    def test_get_and_lock_queue_file__no_remote_file(self):
        """
        A missing remote queue file is not fatal: counted in stats, no lock manipulations in S3.
        """


        class ClientError(Exception):
            pass


        self.scheduler.s3_client.exceptions.ClientError = ClientError
        self.scheduler.s3_client.download_file.side_effect = ClientError("404 Not Found")

        r = self.scheduler.get_and_lock_queue_file()

        self.assertEqual(r, self.scheduler.local_queue_file)
        self.assertEqual(self.scheduler.stats['non_existing_remote_queue'], 1)
        self.scheduler.s3_client.copy_object.assert_not_called()
        self.scheduler.s3_client.delete_object.assert_not_called()


    def test_upload_and_unlock_queue_file__uploads_existing_local_file(self):
        self.put_local_file()

        self.scheduler.upload_and_unlock_queue_file()

        self.scheduler.s3_client.upload_file.assert_called_once_with(
                Filename=self.scheduler.local_queue_file, Bucket=self.scheduler._queue_bucket,
                Key=self.scheduler.remote_queue_file)
        self.scheduler.s3_client.delete_object.assert_called_once_with(
                Bucket=self.scheduler._queue_bucket, Key=self.scheduler.remote_queue_locked_file)


    def test_upload_and_unlock_queue_file__no_remote_locked_file(self):
        """
        Missing remote locked file (e.g. a fresh queue) must not raise.
        """


        class ClientError(Exception):
            pass


        self.scheduler.s3_client.exceptions.ClientError = ClientError
        self.scheduler.s3_client.delete_object.side_effect = ClientError("404 Not Found")

        self.scheduler.upload_and_unlock_queue_file()

        self.scheduler.s3_client.upload_file.assert_not_called()
        self.scheduler.s3_client.delete_object.assert_called_once()


    def test_clean_tmp(self):
        self.put_local_file()
        self.assertTrue(os.path.isfile(self.scheduler.local_queue_file))

        self.scheduler.clean_tmp()

        self.assertFalse(os.path.isfile(self.scheduler.local_queue_file))


    def test_clean_tmp__custom_file_name(self):
        self.put_local_file(self.FNAME)

        self.scheduler.clean_tmp(self.FNAME)

        self.assertFalse(os.path.isfile(self.FNAME))


    def test_set_queue_file__explicit_name(self):
        self.scheduler.set_queue_file('some_sibling_file.txt')
        self.assertEqual(self.scheduler._queue_file_name, 'some_sibling_file.txt')
        self.assertEqual(self.scheduler.local_queue_file, '/tmp/some_sibling_file.txt')


    def test_get_db_field_name(self):
        # setUp() replaces the method with a stub lambda. Remove it to test the real one.
        del self.scheduler.get_db_field_name
        self.scheduler.task_client.get_db_field_name.return_value = 'mapped_field'

        self.assertEqual(self.scheduler.get_db_field_name('task_id'), 'mapped_field')
        self.scheduler.task_client.get_db_field_name.assert_called_once_with('task_id')


    def test_parse_job_to_file__raises_if_local_file_already_exists(self):
        self.put_local_file()

        with self.assertRaises(RuntimeError) as exc:
            self.scheduler.parse_job_to_file({'lambda_name': self.LABOURER.id})

        self.assertIn("already having some unprocessed file", str(exc.exception))


    def test_parse_job_to_file__raises_for_unregistered_labourer(self):
        self.scheduler.task_client.get_labourer.return_value = None

        with self.assertRaises(RuntimeError) as exc:
            self.scheduler.parse_job_to_file({'lambda_name': 'unregistered_function'})

        self.assertIn("Invalid (unregistered) Labourer", str(exc.exception))


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

        self.scheduler.task_client.create_task.return_value = {'task_id': 123,
                                                               'labourer_id': SAMPLE_SIMPLE_JOB['lambda_name'],
                                                               **SAMPLE_SIMPLE_JOB}

        with patch('sosw.scheduler.Scheduler._sleeptime_for_dynamo', new_callable=PropertyMock) as mock_sleeptime:
            mock_sleeptime.return_value = 0.0001

            r = self.scheduler(json.dumps(SAMPLE_SIMPLE_JOB))
            print(r)

        self.scheduler.task_client.create_task.assert_called_once()

        self.scheduler.s3_client.download_file.assert_not_called()
        self.scheduler.s3_client.copy_object.assert_not_called()

        self.scheduler.s3_client.upload_file.assert_called_once()
        self.scheduler.s3_client.delete_object.assert_called_once()


    def test_call__as_sibling_uses_file_name_from_job(self):
        """
        When called as a sibling (with `file_name` in the job), the Scheduler must continue processing
        the received queue file instead of parsing the job to a new one.
        """

        self.scheduler.parse_job_to_file = MagicMock()
        self.scheduler.process_file = MagicMock()

        self.scheduler({'lambda_name': self.LABOURER.id, 'file_name': 'tasks_queue_SIBLING.txt'})

        self.assertEqual(self.scheduler._queue_file_name, 'tasks_queue_SIBLING.txt')
        self.scheduler.parse_job_to_file.assert_not_called()
        self.scheduler.process_file.assert_called_once()


    def test_sleeptime_for_dynamo__on_demand_table(self):
        """
        For on-demand billing the get_capacity() returns zeroes and the sleeptime must be zero.
        """

        self.scheduler.task_client.dynamo_db_client.get_capacity.return_value = {'read': 0, 'write': 0}
        self.assertEqual(self.scheduler._sleeptime_for_dynamo, 0)


    def test_sleeptime_for_dynamo(self):

        self.scheduler.task_client.dynamo_db_client.get_capacity.return_value = {'read': 10, 'write': 10}
        self.assertEqual(round(self.scheduler._sleeptime_for_dynamo, 2), 0.07)

        self.scheduler.task_client.dynamo_db_client.get_capacity.return_value = {'read': 10, 'write': 25}
        self.assertEqual(round(self.scheduler._sleeptime_for_dynamo, 2), 0.01)

        self.scheduler.task_client.dynamo_db_client.get_capacity.return_value = {'read': 10, 'write': 50}
        self.assertEqual(round(self.scheduler._sleeptime_for_dynamo, 2), 0)


    def test_apply_job_schema(self):
        self.scheduler.config['job_schema_variants']['sample_schema_name'] = {
            'chunkable_attrs': [
                ('a', {}),
            ]
        }

        self.scheduler.parse_job_to_file = MagicMock()
        self.scheduler.process_file = MagicMock()
        self.scheduler({'job': {
            'lambda_name': 'test_lambda',
            'job_schema_name': 'sample_schema_name'
            },
        })
        self.assertEqual(self.scheduler.config['job_schema']['chunkable_attrs'][0][0], 'a')


    def test_apply_job_schema__default(self):
        self.scheduler.parse_job_to_file = MagicMock()
        self.scheduler.process_file = MagicMock()
        self.scheduler({'job': {
            'lambda_name': 'test_lambda',
            },
        })

        self.assertEqual(self.scheduler.config['job_schema']['chunkable_attrs'][0][0], 'b')


    def test_apply_job_schema__default_preserved(self):
        """
        First test checks a specific job schema name.
        Second test checks if after calling the scheduler again we overwrite the config and use the default
        specific job schema.

        """

        self.scheduler.config['job_schema_variants']['sample_schema_name'] = {
            'chunkable_attrs': [
                ('a', {}),
            ]
        }

        self.scheduler.parse_job_to_file = MagicMock()
        self.scheduler.process_file = MagicMock()
        self.scheduler({'job': {
            'lambda_name': 'test_lambda',
            'job_schema_name': 'sample_schema_name'
            },
        })

        self.assertEqual(self.scheduler.config['job_schema']['chunkable_attrs'][0][0], 'a')
        self.scheduler({'job': {
            'lambda_name': 'test_lambda',
            },
        })

        self.assertEqual(self.scheduler.config['job_schema']['chunkable_attrs'][0][0], 'b')


    def test_apply_job_schema__chunkable_attrs_reinitialized(self):
        """
        Check that the job schema variant was applied and
        that its job schema chunkable_attrs have been reinitialized to self.chunkable_attrs.

        """

        self.scheduler.config['job_schema_variants']['sample_schema_name'] = {
            'chunkable_attrs': [
                ('a', {}),
                ('b', {}),
                ('c', {}),
            ]
        }

        self.scheduler.parse_job_to_file = MagicMock()
        self.scheduler.process_file = MagicMock()
        self.scheduler({'job': {
            'lambda_name': 'test_lambda',
            'job_schema_name': 'sample_schema_name'
            },
        })

        expected_chunkable_attrs = ['a', 'b', 'c']

        for index, value in enumerate(expected_chunkable_attrs):
            self.assertEqual(self.scheduler.config['job_schema']['chunkable_attrs'][index][0], value)

        self.assertEqual(self.scheduler.chunkable_attrs, expected_chunkable_attrs)

        self.scheduler({'job': {
            'lambda_name': 'test_lambda',
            },
        })

        self.assertEqual(self.scheduler.config['job_schema']['chunkable_attrs'][0][0], 'b')
        self.assertEqual(self.scheduler.chunkable_attrs, ['b'])
