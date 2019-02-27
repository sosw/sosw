import boto3
import logging
import time
import unittest
import os

from collections import defaultdict
from unittest.mock import MagicMock, patch


logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"

from sosw.managers.ecology import EcologyManager
from sosw.test.variables import TEST_CONFIG

class ecology_manager_UnitTestCase(unittest.TestCase):

    TEST_CONFIG = TEST_CONFIG['ecology_client_config']

    def setUp(self):
        """
        We keep copies of main parameters here, because they may differ from test to test and cleanup needs them.
        This is responsibility of the test author to update these values if required from test.
        """

        self.HASH_KEY = ('hash_col', 'S')
        self.RANGE_KEY = ('range_col', 'N')
        self.KEYS = ('hash_col', 'range_col')
        self.table_name = 'autotest_dynamo_db'

        self.patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.patcher.start()

        self.config = self.TEST_CONFIG.copy()
        self.manager = EcologyManager(custom_config=self.config)


    def tearDown(self):
        self.patcher.stop()


    def test_eco_statuses(self):
        self.assertEqual(set(self.manager.eco_statuses), set(range(5)))
