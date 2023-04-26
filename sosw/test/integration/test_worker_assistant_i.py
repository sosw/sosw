import datetime
import logging
import os
import unittest

from sosw.worker_assistant import WorkerAssistant as Processor
from sosw.test.variables import TEST_TASK_CLIENT_CONFIG, TEST_WORKER_ASSISTANT_CONFIG
from sosw.test.helpers_test_dynamo_db import get_autotest_ddb_name, create_test_ddb, drop_test_ddb, get_table_setup, \
    add_gsi, clean_dynamo_table
from unittest.mock import MagicMock, patch

logging.getLogger('botocore').setLevel(logging.WARNING)

os.environ["STAGE"] = "test"
os.environ["autotest"] = "True"


class WorkerAssistant_IntegrationTestCase(unittest.TestCase):

    TEST_CONFIG = TEST_TASK_CLIENT_CONFIG

    autotest_dynamo_db_setup = get_table_setup(hash_key=('task_id', 'S'))
    autotest_dynamo_db_with_index_setup = add_gsi(setup=autotest_dynamo_db_setup, index_name='sosw_tasks_greenfield',
                                                  hash_key=('labourer_id', 'S'), range_key=('greenfield', 'N'))
    autotest_dynamo_db_meta_setup = get_table_setup(hash_key=('task_id', 'S'), range_key=('created_at', 'N'))


    @classmethod
    def setUpClass(cls) -> None:
        # Creation of Dynamo table
        logging.info('Setting up table with structure: %s', cls.autotest_dynamo_db_with_index_setup)
        create_test_ddb(table_structure=cls.autotest_dynamo_db_with_index_setup)


    @classmethod
    def tearDownClass(cls) -> None:

        # Deletion of Dynamo table
        drop_test_ddb(table_name=get_autotest_ddb_name())


    def setUp(self):
        """
        We keep copies of main parameters here, because they may differ from test to test and cleanup needs them.
        This is responsibility of the test author to update these values if required from test.
        """
        self.config = self.TEST_CONFIG.copy()

        self.get_config_patcher = patch("sosw.app.get_config")
        self.get_config_patch = self.get_config_patcher.start()
        self.get_config_patch.return_value = {}

        TEST_WORKER_ASSISTANT_CONFIG['dynamo_db_config']['table_name'] = get_autotest_ddb_name()
        self.processor = Processor(custom_config=TEST_WORKER_ASSISTANT_CONFIG)
        self.processor.meta_handler = MagicMock()


    def tearDown(self):
        """
        """

        # We have to kill processor first of all, otherwise it keeps connection alive.
        # If not processor - no problem. :)
        try:
            del self.processor
        except:
            pass

        clean_dynamo_table(table_name=get_autotest_ddb_name(), keys=('task_id',))
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
