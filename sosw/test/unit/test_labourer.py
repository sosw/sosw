import os
import time
import unittest

from unittest.mock import patch
from sosw.labourer import Labourer


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class Labourer_UnitTestCase(unittest.TestCase):


    def setUp(self):
        self.labourer = Labourer(id=42, arn='arn::aws::lambda')


    def test_init(self):

        self.assertEqual(self.labourer.id, 42)
        self.assertEqual(self.labourer.arn, 'arn::aws::lambda')


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
        self.assertEqual(self.labourer.duration, 900)


    def test_set_defaults_overrides(self):
        lab = Labourer(id=42, duration=300)
        self.assertEqual(lab.duration, 300)


    def test_get_attr(self):

        self.assertRaises(ValueError, self.labourer.get_attr, 'invalid')
        self.assertRaises(AttributeError, self.labourer.get_attr, 'start')


    def test_set_custom_attributes(self):

        self.assertIsNone(getattr(self.labourer, 'start', None))
        self.labourer.set_custom_attribute('start', time.time())

        self.assertLessEqual(self.labourer.start, time.time())
