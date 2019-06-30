import boto3
import os
import unittest

from copy import deepcopy
from unittest.mock import patch

from ..app import Processor
from ..components.sns import SnsManager
from ..components.siblings import SiblingsManager


os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class app_TestCase(unittest.TestCase):
    TEST_CONFIG = {
        'test':            True,
        'siblings_config': {'test': True},
    }


    def setUp(self):
        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        config = deepcopy(self.TEST_CONFIG)
        self.processor = Processor(custom_config=config)


    def tearDown(self):
        self.patcher.stop()


    def test_app_init(self):
        self.assertTrue(True)


    def test_app_init__with_some_clients(self):
        custom_config = deepcopy(self.TEST_CONFIG)
        custom_config.update({
            'init_clients': ['Sns', 'Siblings']
        })

        processor = Processor(custom_config=custom_config)
        self.assertIsInstance(getattr(processor, 'sns_client'), SnsManager,
                              "SnsManager was not initialized. Probably boto3 sns instead of it.")
        self.assertIsNotNone(getattr(processor, 'siblings_client'))


    def test_app_init__boto_and_components_custom_clients(self):
        custom_config = deepcopy(self.TEST_CONFIG)
        custom_config.update({
            'init_clients': ['lambda', 'Siblings']
        })

        processor = Processor(custom_config=custom_config)
        self.assertIsInstance(getattr(processor, 'siblings_client'), SiblingsManager)

        # Clients of boto3 will not be exactly of same type (something dynamic in boto3), so we can't compare classes.
        # Let us assume that checking the class_name is enough for this test.
        self.assertEqual(str(type(getattr(processor, 'lambda_client'))), str(type(boto3.client('lambda'))))


    def test_app_init__with_some_invalid_client(self):
        custom_config = {
            'init_clients': ['NotExists', 'Sns']
        }
        self.assertRaises(RuntimeError, Processor, custom_config=custom_config)
