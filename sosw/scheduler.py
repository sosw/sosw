__all__ = ['Scheduler']

__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "0.1"
__license__ = "MIT"
__status__ = "Development"

import boto3
import logging
import math
import os

from importlib import import_module
from collections import defaultdict
from typing import List, Optional, Dict

from sosw.app import Processor
from sosw.labourer import Labourer
from sosw.managers.task import TaskManager


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Scheduler(Processor):
    """
    Scheduler is converting business jobs to one or multiple Worker tasks.
    """

    DEFAULT_CONFIG = {
        'init_clients': ['Task', 's3', 'Sns'],
        'labourers':    {
            # 'some_function': {
            #     'arn': 'arn:aws:lambda:us-west-2:0000000000:function:some_function',
            #     'max_simultaneous_invocations': 10,
            # }
        },
        's3_prefix':    'sosw/scheduler',
        'queue_file':   'tasks_queue.txt',
        'queue_bucket': 'autotest-bucket',
    }


    def __call__(self, event):
        pass


    def parse_job(self, job):
        pass


    def process_file(self, fname: str):

        file_name = self.get_and_lock_queue_file()
        # while context = OK and rows remaining
        #     read 100 rows
        #     process 100 rows
        #     update_file
        # upload remaining file
        # remove the _locked file.
        pass


    def get_and_lock_queue_file(self):
        """
        Download the version of queue file from S3 and move the file in S3 to `locked_` by prefix state.

        :return: Local path to the downloaded file.
        """

        try:
            self.s3_client.download_file(Bucket=self._queue_bucket, Key=self._remote_queue_file,
                                         Filename=self._local_queue_file)
        except self.s3_client.exceptions.ClientError:
            self.stats['non_existing_remote_queue'] += 1
            logger.exception(f"Not found remote file to download")

        else:
            self.s3_client.copy_object(Bucket=self._queue_bucket,
                                       CopySource=f"{self._queue_bucket}/{self._remote_queue_file}",
                                       Key=self._remote_queue_locked_file)

            self.s3_client.delete_object(Bucket=self._queue_bucket, Key=self._remote_queue_file)

            logger.debug(f"Downloaded a copy of {self._local_queue_file} for processing "
                         f"and moved the remote one to {self._remote_queue_locked_file}.")

            return f"/tmp/{self.config['queue_file']}"


    def upload_and_unlock_queue_file(self):
        """
        Upload the local queue file to S3 and remove the `locked_` by prefix copy if it exists.

        # TODO Should first validate that the `locked` belongs to you. Your should probably abandon everything if not.
        # TODO Otherwise your `_remote_queue_file` will likely get overwritten by someone.
        """

        self.s3_client.upload_file(Filename=self._local_queue_file, Bucket=self._queue_bucket,
                                   Key=self._remote_queue_file)

        try:
            self.s3_client.delete_object(Bucket=self._queue_bucket, Key=self._remote_queue_locked_file)
        except self.s3_client.exceptions.ClientError:
            logger.debug(f"No remote locked file to remove: {self._remote_queue_locked_file}. This is probably new.")


    @property
    def _queue_bucket(self):
        """ Name of S3 bucket for file with queue of tasks not yet in DynamoDB. """
        return self.config['queue_bucket']


    @property
    def _local_queue_file(self):
        """ Full path of local file with queue of tasks not yet in DynamoDB. """
        return f"/tmp/{self.config['queue_file'].strip('/')}"


    @property
    def _remote_queue_file(self):
        """ Full S3 Key of file with queue of tasks not yet in DynamoDB. """
        return f"{self.config['s3_prefix'].strip('/')}/{self.config['queue_file'].strip('/')}"


    @property
    def _remote_queue_locked_file(self):
        """
        Full S3 Key of file with queue of tasks not yet in DynamoDB in the `locked` state.
        Concurrent processes should not touch it.

        # TODO Make sure this has some invocation ID of current run or smth.
        # TODO Otherwise some parallel process may just write a new _remote_queue_file and kill this one.
        """
        return f"{self.config['s3_prefix'].strip('/')}/locked_{self.config['queue_file'].strip('/')}"
