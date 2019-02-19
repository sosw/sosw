import datetime
import logging
import shutil
import time
import unittest
import os


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from ..helpers import *


class helpers_UnitTestCase(unittest.TestCase):

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_validate_account_to_dashed_valid(self):
        """
        Test validate_account_to_dashed() with valid inputs.
        :return:
        """

        self.assertEqual(validate_account_to_dashed("123-456-7890"), "123-456-7890")
        self.assertEqual(validate_account_to_dashed("1234567890"), "123-456-7890")
        self.assertEqual(validate_account_to_dashed(1234567890), "123-456-7890")


    def test_validate_account_to_dashed_invalid(self):
        """
        Test validate_account_to_dashed() with invalid inputs.
        :return:
        """

        self.assertRaises(TypeError, validate_account_to_dashed)
        self.assertRaises(ValueError, validate_account_to_dashed, "")
        self.assertRaises(ValueError, validate_account_to_dashed, "1")
        self.assertRaises(ValueError, validate_account_to_dashed, "12345678901")


    def test_validate_list_of_numbers_from_csv(self):
        """
        Test behaviour of validate_list_of_numbers_from_csv() with different inputs.
        :return:
        """

        self.assertRaises(TypeError, validate_list_of_numbers_from_csv)
        self.assertEqual(validate_list_of_numbers_from_csv("123,   234 ,,  ,345, asd, 123$@#!, "), [123, 234, 345])
        self.assertEqual(validate_list_of_numbers_from_csv(123), [123])
        self.assertEqual(validate_list_of_numbers_from_csv([123, 234, '345']), [123, 234, 345])
        self.assertEqual(validate_list_of_numbers_from_csv(""), [])


    def test_get_one_or_none_from_dict(self):
        """
        Test behavior of method.
        """

        INPUT = {
            'k1_str':   'v1',
            'k2_int':   3,
            'k2_str':   '3',
            'k3_float': 1.2,
            'l0s':      [],
            'li1s':     [1],
            'li2s':     [1, 2],
            'ls1s':     ['a'],
            'ls2s':     ['a', 'b'],
        }

        self.assertRaises(ValueError, get_one_or_none_from_dict, 'not_dict', 'n1')
        self.assertRaises(ValueError, get_one_or_none_from_dict, INPUT, 1)
        self.assertRaises(ValueError, get_one_or_none_from_dict, INPUT, 'k1_str', int)
        self.assertRaises(ValueError, get_one_or_none_from_dict, INPUT, 'li2')
        self.assertRaises(ValueError, get_one_or_none_from_dict, INPUT, 'ls2')

        self.assertEqual(get_one_or_none_from_dict(INPUT, 'k1_str'), 'v1')
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'k2_int'), 3)
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'k2_int', int), 3)
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'k2_str', int), 3)
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'k2_str'), '3')
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'k3_float'), 1.2)
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'k3_float', float), 1.2)
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'k3_float', int), 1)
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'k3_float', str), '1.2')
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'li1', str), '1')
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'li1', int), 1)
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'ls1'), 'a')
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'l0'), None)
        self.assertEqual(get_one_or_none_from_dict(INPUT, 'l0s'), None)


    def test_get_one_from_dict(self):
        """
        Test behavior of method.
        As long as the method is a wrapping for get_one_or_none_from_dict(), we run same tests + several specific ones.
        """

        INPUT = {
            'k1_str':   'v1',
            'k2_int':   3,
            'k2_str':   '3',
            'k3_float': 1.2,
            'l0s':      [],
            'li1s':     [1],
            'li2s':     [1, 2],
            'ls1s':     ['a'],
            'ls2s':     ['a', 'b'],
        }

        self.assertRaises(ValueError, get_one_from_dict, 'not_dict', 'n1')
        self.assertRaises(ValueError, get_one_from_dict, INPUT, 1)
        self.assertRaises(ValueError, get_one_from_dict, INPUT, 'k1_str', int)
        self.assertRaises(ValueError, get_one_from_dict, INPUT, 'li2')
        self.assertRaises(ValueError, get_one_from_dict, INPUT, 'ls2')

        self.assertEqual(get_one_from_dict(INPUT, 'k1_str'), 'v1')
        self.assertEqual(get_one_from_dict(INPUT, 'k2_int'), 3)
        self.assertEqual(get_one_from_dict(INPUT, 'k2_int', int), 3)
        self.assertEqual(get_one_from_dict(INPUT, 'k2_str', int), 3)
        self.assertEqual(get_one_from_dict(INPUT, 'k2_str'), '3')
        self.assertEqual(get_one_from_dict(INPUT, 'k3_float'), 1.2)
        self.assertEqual(get_one_from_dict(INPUT, 'k3_float', float), 1.2)
        self.assertEqual(get_one_from_dict(INPUT, 'k3_float', int), 1)
        self.assertEqual(get_one_from_dict(INPUT, 'k3_float', str), '1.2')
        self.assertEqual(get_one_from_dict(INPUT, 'li1', str), '1')
        self.assertEqual(get_one_from_dict(INPUT, 'li1', int), 1)
        self.assertEqual(get_one_from_dict(INPUT, 'ls1'), 'a')

        # These are specific exceptions for get_one_from_dict.
        self.assertRaises(ValueError, get_one_from_dict, INPUT, 'l0')
        self.assertRaises(ValueError, get_one_from_dict, INPUT, 'l0s')


    def test_get_list_of_multiple_or_one_or_empty_from_dict(self):
        """
        Test behavior of method.
        """

        INPUT = {
            'account':             123,
            'accounts':            [123, 234],
            'only_singular':       '567',
            'string':              ['a'],
            'strings':             ['a', 'b'],
            'not_numeric':         'a123',
            'list_with_mixed':     [123, '234'],
            'list_with_mixed_bad': [123, 'abc'],
        }

        self.assertRaises(ValueError, get_list_of_multiple_or_one_or_empty_from_dict, 'not_dict', 'n1')
        self.assertRaises(ValueError, get_list_of_multiple_or_one_or_empty_from_dict, INPUT,
                          1)  # We require name to be string. No int keys.
        self.assertRaises(ValueError, get_list_of_multiple_or_one_or_empty_from_dict, INPUT, 'not_numeric', int)
        self.assertRaises(ValueError, get_list_of_multiple_or_one_or_empty_from_dict, INPUT, 'list_with_mixed_bad', int)
        self.assertRaises(ValueError, get_list_of_multiple_or_one_or_empty_from_dict, INPUT, 'string', int)

        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'accounts'), [123, 234])
        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'account'), [123])

        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'strings', str), ['a', 'b'])
        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'string', str), ['a'])
        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'string'), ['a'])

        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'only_singular'), ['567'])
        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'only_singulars'), ['567'])
        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'only_singulars', int), [567])
        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'only_singulars', int), [567])

        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'list_with_mixed', int), [123, 234])
        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'list_with_mixed'), [123, '234'])
        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'list_with_mixed_bad'), [123, 'abc'])

        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'no_such_key'), [])
        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'no_such_key', int), [])
        self.assertEqual(get_list_of_multiple_or_one_or_empty_from_dict(INPUT, 'no_such_key', str), [])


    def test_rstrip_all(self):
        masks = ['[]', '[/]', '"/"', '""', '- MB', '[Mix]', '[MIX]']

        # With lists of patterns
        self.assertEqual(rstrip_all('Name - MB', masks), 'Name')
        self.assertEqual(rstrip_all('Name []', masks), 'Name')
        self.assertEqual(rstrip_all('Name [][/]"/"', masks), 'Name')
        self.assertEqual(rstrip_all('Name [][baz][/]"/"', masks), 'Name [][baz]')
        self.assertEqual(rstrip_all('Name [] - MB', masks), 'Name')
        self.assertEqual(rstrip_all('""Name [] - MB', masks), '""Name')
        self.assertEqual(rstrip_all('[Mix] Name [] - MB', masks), '[Mix] Name')
        self.assertEqual(rstrip_all('Name []   "/"- MB', masks), 'Name')
        self.assertEqual(rstrip_all('Name    [Baz]', masks), 'Name    [Baz]')
        self.assertEqual(rstrip_all('Name   "" [Baz]', masks), 'Name   "" [Baz]')

        # With plain string pattern
        self.assertEqual(rstrip_all('Name []', "[]"), 'Name')
        self.assertEqual(rstrip_all('Name [0-9]', "[0-9]"), 'Name')
        self.assertEqual(rstrip_all('Name 5', "[0-9]"), 'Name 5')
        self.assertEqual(rstrip_all('Name   []   ', "[]"), 'Name')
        self.assertEqual(rstrip_all('Name [] baz', "[]"), 'Name [] baz')
        self.assertEqual(rstrip_all('Name [] baz[]', "[]"), 'Name [] baz')
        self.assertEqual(rstrip_all('Name []', "[][baz]"), 'Name []')
        self.assertEqual(rstrip_all('[] Name []', "[]"), '[] Name')


    def test_validate_date_list_from_event_or_days_back(self):
        """
        Test behavior of method.
        """

        TESTS = [
            ('2018-01-01', [datetime.date(2018, 1, 1)]),
            (['2018-01-02'], [datetime.date(2018, 1, 2)]),
            (['2018-01-03', '2018-01-04'], [datetime.date(2018, 1, 3), datetime.date(2018, 1, 4)]),
        ]

        for input, value in TESTS:
            self.assertEqual(validate_date_list_from_event_or_days_back({'date_list': input}), value)


    def test_recursive_matches_soft(self):
        SRC = {'bar': [{'foo': 'fval'}, {'page': {'oid': 123, 'code': '123'}}, {'page': {'oid': 'a'}}], 'name': 'test'}

        self.assertTrue(recursive_matches_soft(SRC, 'bar.page.oid', 123))
        self.assertTrue(recursive_matches_soft(SRC, 'bar.page.oid', 'a'))
        self.assertTrue(recursive_matches_soft(SRC, 'bar.foo', 'fval'))
        self.assertTrue(recursive_matches_soft(SRC, 'name', 'test'))

        self.assertFalse(recursive_matches_soft(SRC, 'name', 'invalid'))
        self.assertFalse(recursive_matches_soft(SRC, 'invalid', 1))
        self.assertFalse(recursive_matches_soft(SRC, 'bar.page.oid', 1))
        self.assertFalse(recursive_matches_soft(SRC, 'bar.page.oid', '123'))
        self.assertFalse(recursive_matches_soft(SRC, 'bar.page.oid', "invalid"))
        self.assertFalse(recursive_matches_soft(SRC, 'bar.page.oid', None))
        self.assertFalse(recursive_matches_soft(SRC, 'bar.invalid.oid', None))
        self.assertFalse(recursive_matches_soft(SRC, 'bar.invalid', None))

        self.assertFalse(recursive_matches_soft(SRC, 'bar.page.oid', 123, exclude_key='code', exclude_val='123'),
                         "The matching element also matches exclude attributes")

        # Must specify both 'exclude_key' and 'exclude_val' if any.
        self.assertRaises(AttributeError, recursive_matches_soft, SRC, 'bar.invalid', 1, exclude_key='bar')


    def test_recursive_matches_strict(self):
        SRC = {'bar': [{'page': {'oid': 234}}, {'page': {'oid': 123, 'code': '123'}}], 'name': 'test'}

        self.assertTrue(recursive_matches_strict(SRC, 'bar.page.oid', 123))
        self.assertTrue(recursive_matches_strict(SRC, 'bar.page.oid', 234))
        self.assertTrue(recursive_matches_strict(SRC, 'name', 'test'))

        self.assertFalse(recursive_matches_strict(SRC, 'name', 'invalid'))
        self.assertFalse(recursive_matches_strict(SRC, 'bar.page.oid', 1))

        self.assertFalse(recursive_matches_strict(SRC, 'bar.page.oid', 123, exclude_key='code', exclude_val='123'),
                         "The matching element also matches exclude attributes")

        self.assertRaises(KeyError, recursive_matches_strict, SRC, 'foo.bar', 1)
        self.assertRaises(KeyError, recursive_matches_strict, SRC, 'foo.bar.baz', None)
        self.assertRaises(KeyError, recursive_matches_strict, SRC, 'bar.page.baz', None)

        SRC2 = {'bar': [{'different': {'classes': 'in_list'}}, {'page': {'oid': 123, 'code': '123'}}], 'name': 'test'}
        self.assertRaises(KeyError, recursive_matches_strict, SRC2, 'bar.page.oid', 1)

        # Must specify both 'exclude_key' and 'exclude_val' if any.
        self.assertRaises(AttributeError, recursive_matches_soft, SRC, 'bar.invalid', 1, exclude_key='bar')


    def test_validate_datetime_from_something(self):
        self.assertEqual(validate_datetime_from_something(datetime.datetime(9999, 12, 30, 23, 59, 59)),
                         datetime.datetime(9999, 12, 30, 23, 59, 59),
                         "Failed with big, but valid datetime to datetime")


        self.assertEqual(validate_datetime_from_something(datetime.date(2018, 7, 5)), datetime.datetime(2018, 7, 5),
                         "Failed transforming date to datetime")

        self.assertEqual(validate_datetime_from_something(1000), datetime.datetime.fromtimestamp(1000),
                         "Failed from epoch time")

        t = time.time()
        self.assertEqual(validate_datetime_from_something(t * 1000), datetime.datetime.fromtimestamp(t),
                         "Failed with epoch in milliseconds")

        self.assertEqual(validate_datetime_from_something(1000.0), datetime.datetime.fromtimestamp(1000),
                         "Failed from epoch time in float")

        self.assertEqual(validate_datetime_from_something('2018-01-01'), datetime.datetime(2018, 1, 1),
                         "Failed from string YYYY-DD-MM")

        self.assertEqual(validate_datetime_from_something('2018-01-01 10:01:03'),
                                                          datetime.datetime(2018, 1, 1, 10, 1, 3),
                         "Failed from string YYYY-DD-MM HH:MM:SS")

        self.assertRaises(ValueError, validate_datetime_from_something, 'somebadstring')
        self.assertRaises(ValueError, validate_datetime_from_something, 253402300800000)


    def test_validate_date_from_something(self):
        """
        Passing supported types and expecting 'datetime.date' object to returns.
        """
        for input_type in [datetime.datetime.today(),
                      datetime.date.today(),
                      int(time.time()),
                      time.time(),
                      str(datetime.date.today()),
                      str(datetime.datetime.today())]:

            self.assertIsInstance(validate_date_from_something(input_type), datetime.date)


    def test_negative_validate_date_from_something(self):
        """
        Passing unsupported types and expecting ValueError.
        """

        for input_type in ["1, 2", [1, 2], {1: 2}, (1, 2)]:
            self.assertRaises(ValueError, validate_date_from_something, input_type)


    def test_recursive_match_extract(self):
        SRC = {"bar":[{"page":{"oid":234}},{"page":{"code":"exclude_me","id":123}},
                      {"page":{"code":"ok","id":333}}],"name":"test"}

        self.assertEqual(recursive_matches_extract(SRC, 'name'), "test")
        self.assertEqual(recursive_matches_extract(SRC, 'bar.page.oid'), 234)
        self.assertEqual(recursive_matches_extract(SRC, 'bar.page.id'), 123, "The first one in list")
        self.assertEqual(recursive_matches_extract(SRC, 'bar.page.id', exclude_key='code', exclude_val='exclude_me'),
                         333, "Exclude did not work.")

        self.assertIsNone(recursive_matches_extract(SRC, 'na'))
        self.assertIsNone(recursive_matches_extract(SRC, 'bar.baz'))
        self.assertIsNone(recursive_matches_extract(SRC, 'bar.page.foo'))

        self.assertRaises(AttributeError, recursive_matches_extract, SRC, 'foo.bar.baz', exclude_key=1)
        self.assertRaises(AttributeError, recursive_matches_extract, SRC, 'foo.bar.baz', exclude_val=1)


    def test_chunks(self):
        list_input = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
        list_input_2 = [[1, 2, 3], ['a'], [True, False]]
        number_of_items_in_chunk = 2
        self.assertEqual(len(list(chunks(list_input, number_of_items_in_chunk))), 4, "chunked to a wrong amount")
        self.assertEqual(len(list(chunks(list_input_2, number_of_items_in_chunk))), 2, "chunked to a wrong amount")


    def test_validate_string_matches_datetime_format(self):
        date_str = "2018/05/15"
        date_format = "%Y/%m/%d"
        validate_string_matches_datetime_format(date_str, date_format)

        date_str = "2018/05/15 04:23"
        date_format = "%Y/%m/%d %H:%M"
        validate_string_matches_datetime_format(date_str, date_format)

        date_str = "04:23"
        date_format = "%H:%M"
        validate_string_matches_datetime_format(date_str, date_format)

        date_str = "2018/13/15"
        date_format = "%Y/%m/%d"
        with self.assertRaises(ValueError):
            validate_string_matches_datetime_format(date_str, date_format)

        date_str = "2018/05/15"
        date_format = "%/%m/%d"
        with self.assertRaises(ValueError):
            validate_string_matches_datetime_format(date_str, date_format)

        date_str = "25:00"
        date_format = "%H:%M"
        with self.assertRaises(ValueError):
            validate_string_matches_datetime_format(date_str, date_format)


    def test_convert_string_to_words(self):
        self.assertEquals(convert_string_to_words('Best Dating NY'), 'best,dating,ny')
        self.assertEquals(convert_string_to_words('   Best     Dating NY'), 'best,dating,ny')
        self.assertEquals(convert_string_to_words('Best 42 daTing sites     '), 'best,42,dating,sites')


    def test_construct_dates_from_event__conflict_of_attributes(self):
        self.assertRaises(AttributeError, construct_dates_from_event, {'st_date': '2018-01-01', 'days_back': 10}), \
                    "Not raised conflict of attributes"


    def test_construct_dates_from_event__missing_attributes(self):
        self.assertRaises(AttributeError, construct_dates_from_event, {'bad_event': 'missing st_date and days_back'}), \
                    "Not raised missing attributes in event"


    def test_construct_dates_from_event__ok(self):

        TESTS = {
            (datetime.date(2019, 1, 1), datetime.date(2019, 1, 10)): {'st_date': '2019-01-01', 'en_date': '2019-01-10'},
            (datetime.date(2019, 1, 1), datetime.date.today()): {'st_date': '2019-01-01'},
            (datetime.date(2018, 12, 31), datetime.date(2019, 1, 10)): {'days_back': 10, 'en_date': '2019-01-10'},
            (datetime.date(2019, 1, 1), datetime.date(2019, 1, 10)): {'days_back': '9', 'en_date': '2019-01-10'},
            (datetime.date.today() - datetime.timedelta(days=10), datetime.date.today()): {'days_back': 10},
        }

        for expected, payload in TESTS.items():
            self.assertEqual(expected, construct_dates_from_event(payload))


if __name__ == '__main__':
    unittest.main()
