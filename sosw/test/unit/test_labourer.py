import os
import unittest

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

