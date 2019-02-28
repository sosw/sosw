import os
import time
import unittest

from unittest.mock import patch
from sosw.labourer import Labourer


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Labourer_UnitTestCase(unittest.TestCase):

    def test_init(self):
        lab = Labourer(id=42, arn='arn::aws::lambda')

        self.assertEqual(lab.id, 42)
        self.assertEqual(lab.arn, 'arn::aws::lambda')


    def test_init_attrs(self):
        lab = Labourer(id='foo', arn='arn::aws::lambda', max_invocations=13)

        self.assertEqual(lab.id, 'foo')
        self.assertEqual(lab.arn, 'arn::aws::lambda')
        self.assertEqual(lab.max_invocations, 13)


    def test_init__strict_raises(self):
        self.assertRaises(AttributeError, Labourer, **{'foo': 'bar', 'strict': True}), \
        f"Labourer supports only {Labourer.ATTRIBUTES}"


    def test_set_defaults__called(self):
        with patch('sosw.labourer.Labourer.set_defaults')  as sd:
            lab = Labourer(id=42)
            sd.assert_called_once()


    def test_set_defaults(self):
        lab = Labourer(id=42)
        self.assertEqual(lab.duration, 900)


    def test_set_defaults_overrides(self):
        lab = Labourer(id=42, duration=300)
        self.assertEqual(lab.duration, 300)


    def test_get_timestamps__raises(self):

        lab = Labourer(id=42)
        self.assertRaises(ValueError, lab.get_timestamp, 'invalid')
        self.assertRaises(AttributeError, lab.get_timestamp, 'start')


    def test_set_timestamps(self):
        lab = Labourer(id=42)

        self.assertIsNone(getattr(lab, 'start', None))
        lab.set_timestamp('start', time.time())

        self.assertLessEqual(lab.start, time.time())