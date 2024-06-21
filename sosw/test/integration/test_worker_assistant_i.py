import asyncio
import datetime
import logging
import os
import unittest

from sosw.worker_assistant import WorkerAssistant as Processor
from sosw.test.variables import TEST_TASK_CLIENT_CONFIG, TEST_WORKER_ASSISTANT_CONFIG
from sosw.test.helpers_test_dynamo_db import get_autotest_ddb_name, create_test_ddb, drop_test_ddb, get_table_setup, \
    add_gsi, clean_dynamo_table, autotest_dynamo_db_tasks_setup, autotest_dynamo_db_meta_setup, \
    autotest_dynamo_db_closed_tasks_setup, AutotestDdbManager, get_autotest_ddb_name_with_custom_suffix
from unittest.mock import MagicMock, patch

logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class WorkerAssistant_IntegrationTestCase(unittest.TestCase):

    TEST_CONFIG = TEST_TASK_CLIENT_CONFIG
    autotest_ddbm = None


    @classmethod
    def setUpClass(cls) -> None:
        # Creation of Dynamo table
        cls.autotest_ddbm = AutotestDdbManager()


    @classmethod
    def tearDownClass(cls) -> None:

        asyncio.run(cls.autotest_ddbm.drop_ddbs())


    def setUp(self):
        """
        We keep copies of main parameters here, because they may differ from test to test and cleanup needs them.
        This is responsibility of the test author to update these values if required from test.
        """
        self.config = self.TEST_CONFIG.copy()

        self.get_config_patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.get_config_patcher.start()
        self.get_config_patch.return_value = {}

        self.processor = Processor(custom_config=TEST_WORKER_ASSISTANT_CONFIG)
        self.processor.meta_handler = MagicMock()


    def tearDown(self):
        """
        """

        # We have to kill processor first of all, otherwise it may keep some connections alive.
        try:
            del self.processor
        except:
            pass

        asyncio.run(self.autotest_ddbm.clean_ddbs())
        self.get_config_patcher.stop()


    def test_mark_task_as_completed(self):
        _ = self.processor.get_db_field_name
        task_id = '123'

        initial_task = {_('task_id'): task_id, _('labourer_id'): 'lab', _('greenfield'): 8888, _('attempts'): 2}
        self.processor.dynamo_db_client.put(initial_task)

        between_times = (
            (datetime.datetime.now() - datetime.timedelta(minutes=1)).timestamp(),
            (datetime.datetime.now() + datetime.timedelta(minutes=1)).timestamp()
        )

        self.processor.mark_task_as_completed(task_id)

        changed_task = self.processor.dynamo_db_client.get_by_query({_('task_id'): task_id})[0]

        self.assertTrue(between_times[0] <= changed_task['completed_at'] <= between_times[1],
                        msg=f"NOT {between_times[0]} <= {changed_task['completed_at']} <= {between_times[1]}")


    def test_mark_task_as_failed(self):
        _ = self.processor.get_db_field_name
        task_id = '123'

        initial_task = {_('task_id'): task_id, _('labourer_id'): 'lab', _('greenfield'): 8888, _('attempts'): 0}
        self.processor.dynamo_db_client.put(initial_task)

        self.processor.mark_task_as_failed(task_id, result={'some_info': 33})

        changed_task = self.processor.dynamo_db_client.get_by_query({_('task_id'): task_id}, fetch_all_fields=True)[0]

        self.assertEqual(1, changed_task['failed_attempts'])
        self.assertEqual(33, changed_task['result_some_info'])
