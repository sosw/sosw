__all__ = ['Scheduler']

__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "0.1"
__license__ = "MIT"
__status__ = "Development"

import boto3
import json
import logging
import math
import os
import time

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
        'init_clients':    ['Task', 's3', 'Sns'],
        'labourers':       {
            # 'some_function': {
            #     'arn': 'arn:aws:lambda:us-west-2:0000000000:function:some_function',
            #     'max_simultaneous_invocations': 10,
            # }
        },
        's3_prefix':       'sosw/scheduler',
        'queue_file':      'tasks_queue.txt',
        'queue_bucket':    'autotest-bucket',
        'shutdown_period': 60,
        'rows_to_process': 50,
    }


    def __call__(self, event):
        # FIXME this is a temporary solution. Should take remaining time from Context object.
        self.st_time = time.time()


    def parse_job(self, job: Dict):
        """
        Splits the Job to multiple tasks and writes them down in self._local_queue_file.

        :param dict job:    Payload from Scheduled Rule.
                            Should be already parsed from whatever payload to dict and contain the raw `job`
        """

        labourer = Labourer(id=job['lambda_name'])

        raise Exception


    def extract_job_from_payload(self, event: Dict):
        """ Parse and basically validate job from the event. """

        def load(obj):
            return obj if isinstance(obj, dict) else json.loads(obj)

        jh = load(event)
        job = load(jh['job']) if 'job' in jh else jh

        assert 'lambda_name' in job, f"Job is missing required parameter 'lambda_name': {job}"
        job['lambda_name'] = job['lambda_name']

        return job


    def process_file(self):

        file_name = self.get_and_lock_queue_file()

        if not file_name:
            logger.info(f"No file in queue.")
            return
        else:
            while self.sufficient_execution_time_left:
                data = self.pop_rows_from_file(file_name, rows=self._rows_to_process)
                if not data:
                    break

                for task in data:
                    logger.info(task)
                    self.task_client.create_task(json.loads(task))
                    time.sleep(self._sleeptime_for_dynamo)

            self.upload_and_unlock_queue_file()


    @property
    def _sleeptime_for_dynamo(self):
        # TODO Should use incremental sleeps based on Throttling. Probably interact with EcologyManager.
        return 0.2



    @staticmethod
    def pop_rows_from_file(file_name: str, rows: Optional[int] = 1) -> List[str]:
        """
        Reads the rows from the top of file. Along the way removes them from original file.

        :param str file_name:    File to read.
        :param int rows:        Number of rows to read. Default: 1
        :return:                List of strings read from file top.
        """

        tmp_file = f"/tmp/in_prog_{file_name.replace('/', '_')}"
        result = []

        try:
            with open(file_name) as f, open(tmp_file, "w") as out:
                for _ in range(rows):
                    try:
                        result.append(next(f))
                    except StopIteration:
                        break

                # Writing remaining rows to the temp file.
                for line in f:
                    out.write(line)

            os.remove(file_name)
            os.rename(tmp_file, file_name)
        except FileNotFoundError:
            pass

        return result


    @property
    def _rows_to_process(self):
        return self.config['rows_to_process']


    @property
    def sufficient_execution_time_left(self) -> bool:
        # FIXME this is a temporary solution. Should take remaining time from Context object.
        return (time.time() - self.st_time) < (300 - self.config['shutdown_period'])


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
