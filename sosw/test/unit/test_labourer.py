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


    def test_init__raises(self):
        self.assertRaises(AttributeError, Labourer, **{'foo': 'bar'}), f"Labourer supports only {Labourer.ATTRIBUTES}"
