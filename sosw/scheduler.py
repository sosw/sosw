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
import re
import time

from importlib import import_module
from collections import defaultdict
from collections import OrderedDict
from collections import Iterable
from copy import deepcopy
from typing import List, Set, Tuple, Union, Optional, Dict

from sosw.app import Processor
from sosw.components.helpers import get_list_of_multiple_or_one_or_empty_from_dict
from sosw.labourer import Labourer
from sosw.managers.task import TaskManager


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def single_or_plural(attr):
    """ Simple function. Gives versions with 's' at the end and without it. """
    return list(set([attr, attr.rstrip('s'), f"{attr}s"]))


def plural(attr):
    """ Simple function. Gives plural form with 's' at the end. """
    return f"{attr.rstrip('s')}s"


class InvalidJob(ValueError):
    pass


class Scheduler(Processor):
    """
    Scheduler is converting business jobs to one or multiple Worker tasks.
    """

    DEFAULT_CONFIG = {
        'init_clients':    ['Task', 's3', 'Sns'],
        'task_config':     {
            'labourers': {
                # 'some_function': {
                #     'arn': 'arn:aws:lambda:us-west-2:0000000000:function:some_function',
                #     'max_simultaneous_invocations': 10,
                # }
            },
        },
        's3_prefix':       'sosw/scheduler',
        'queue_file':      'tasks_queue.txt',
        'queue_bucket':    'autotest-bucket',
        'shutdown_period': 60,
        'rows_to_process': 50,
        'job_schema':      {
            'chunkable_attrs': [
                # ('section', {}),
                # ('store', {}),
                # ('product', {}),
            ]
        }
    }

    # these clients will be initialized by Processor constructor
    task_client: TaskManager = None
    s3_client = None
    sns_client = None
    base_query = ...


    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.chunkable_attrs = list([x[0] for x in self.config['job_schema']['chunkable_attrs']])
        assert not any(x.endswith('s') for x in self.chunkable_attrs), \
            f"We do not currently support attributes that end with 's'. " \
            f"In the config you should use singular form of attribute. Received from config: {self.chunkable_attrs}"


    def __call__(self, event):
        # FIXME this is a temporary solution. Should take remaining time from Context object.
        self.st_time = time.time()

        job = self.extract_job_from_payload(event)

        self.parse_job_to_file(job)

        self.process_file()


    def parse_job_to_file(self, job: Dict):
        """
        Splits the Job to multiple tasks and writes them down in self._local_queue_file.

        :param dict job:    Payload from Scheduled Rule.
                            Should be already parsed from whatever payload to dict and contain the raw `job`
        """

        if os.path.isfile(self._local_queue_file):
            logger.critical(f"The current Lambda container is already having some unprocessed file. "
                            f"You probably did not clean it correctly after processing. "
                            f"Should probably just clean the file now, but during beta version we raise an stop "
                            f"processing new ones.")
            raise RuntimeError(f"The current Lambda container is already having some unprocessed file.")

        labourer = self.task_client.get_labourer(labourer_id=job.pop('lambda_name'))

        if not all([self.chunkable_attrs, self.needs_chunking(plural(self.chunkable_attrs[0]), job)]):
            data = [{'labourer_id': labourer.id, **job}]
        else:
            data = self.construct_job_data(job, skeleton={'labourer_id': labourer.id})

        with open(self._local_queue_file, 'w') as f:
            for row in data:
                f.write(f"{json.dumps(row)}\n")


    # def create_tasks(self, labourer: Labourer, data: List):
    #     """
    #     Iterate tasks from `data` and queue them as new tasks for `labourer`.
    #     """
    #
    #     for task in data:
    #         self.task_client.create_task(labourer=labourer, payload=task)

    def validate_list_of_vals(self, data: Union[List, Set, Tuple, Dict]) -> List:
        """
        Supported resulting values: str, int, float.

        Expects a simple iterable of supported values or a dictionary with values = None.
        The keys then are treated as resulting values and if they validate, are returned as a list.
        """

        if isinstance(data, (list, set, tuple)):
            if not all(isinstance(v, (str, int, float)) for v in data):
                raise InvalidJob(f"Job has values with embedded data, but chunking is not requested. "
                                 f"You should either append 'isolate_ATTRIBUTE' flag or make values appropriate. "
                                 f"Should be a flat list or a dict with None - values. Your job was: {data}")
            return data

        elif isinstance(data, dict):
            if not all(v is None for v in data.values()):
                raise InvalidJob(f"Job have values with embedded data, but chunking is not requested. "
                                 f"You should either append 'isolate_ATTRIBUTE' flag or make values appropriate. "
                                 f"Should be a flat list or a dict with None - values. Your job was: {data}")

            return list(data.keys())

        else:
            raise InvalidJob(f"A Job without chunking enabled should have value either as a simple iterable "
                             f"(list, set, tuple), or a dict with all the values = None."
                             f"You provided {type(data)}: {data}")


    def construct_job_data(self, job: dict, skeleton: Dict = None, attr: str = None) -> List:
        """
        Recursively parses a job, validates everything and chunks to simple tasks what should be chunked.
        The Scenario of chunking and isolation is worth another story, so you should put a link here once it is ready.
        """

        skel = deepcopy(skeleton) or {}
        attr = attr or self.chunkable_attrs[0] if self.chunkable_attrs else None

        # We have to return here the full job to let it work correctly with recursive calls.
        if not attr:
            return [job]

        data = []

        logger.debug(f"Testing for chunking {attr} from {job} with skeleton {skeleton}")

        # First of all decide whether we need to chunk current job (or a sub-job if called recursively).
        if self.needs_chunking(plural(attr), job):

            # Next attribute is either name of attribute according to config, or None if we are already in last level.
            next_attr = self.get_next_chunkable_attr(attr)

            # Here and manu places further we support both single and plural versions of attribute names.
            for possible_attr in single_or_plural(attr):
                current_vals = get_list_of_multiple_or_one_or_empty_from_dict(job, possible_attr)
                logger.debug(f"For {possible_attr} we got current_vals: {current_vals} from {job}.")

                # For dictionaries we have to either go deeper recursively, or just flatten keys if values are None-s.
                if all(isinstance(v, dict) for v in current_vals):
                    for val in current_vals:
                        for name, subdata in val.items():
                            logger.debug(f"SubIterating `{name}` with {subdata}")

                            task = deepcopy(skel)
                            task[plural(attr)] = [name]

                            if isinstance(subdata, dict):
                                if not next_attr:
                                    raise InvalidJob(f"Unexpected dictionary for unchunkable attribute: {attr}. "
                                                     f"In order to chunk this, you should support this level in: "
                                                     f"`config.job_schema.chunkable_attrs`. "
                                                     f"If you want to pass custom payload - put it as `payload` in "
                                                     f"your job. Job was: {job}")

                                logger.debug(f"Call recursive for {next_attr} from subdata: {subdata}")
                                data.extend(self.construct_job_data(job=subdata, skeleton=task, attr=next_attr))

                            # If None-s we just add a task. `Name` (which is actually a value in this scenario)
                            # was already added when creating task skeleton.
                            elif subdata is None:
                                logger.debug(f"Appending task to data for {name} from {val}")
                                data.append(task)

                            else:
                                raise InvalidJob(f"Unsupported type of val: {subdata} for attribute {possible_attr}")

                # If current vals are not dictionaries, we just validate that they are flat supported values
                else:
                    vals = self.validate_list_of_vals(current_vals)

                    for val in vals:
                        task = deepcopy(skel)
                        task[plural(attr)] = [val]
                        data.append(task)

        else:
            logger.debug(f"No need for chunking for attr: {attr} in job: {job}. Current skeleton is: {skel}")
            task = deepcopy(skel)

            for a in single_or_plural(attr):
                if a in job:
                    vals = self.validate_list_of_vals(job[a])
                    task[plural(attr)] = vals
                    break
            else:
                logger.error(f"Did not find values for {attr} in job: {job}")
            logger.debug(f"Appending task to data: {task}")
            data.append(task)

        return data


    @staticmethod
    def get_index_from_list(attr, data):
        """ Finds the index ignoring the 's' at the end of attribute. """

        assert isinstance(data, Iterable), f"Non iterable data for get_index_from_list: {data}"
        assert isinstance(attr, str), f"Non-string attr for get_index_from_list: {type(attr)}"

        attrs = single_or_plural(attr)
        for a in attrs:
            try:
                return list(data).index(a)
            except ValueError:
                pass
        raise ValueError(f"Not found {attr} in {data}")


    def get_next_chunkable_attr(self, attr):
        """ Return the next by order after `attr` chunkable attribute. """

        attrs = single_or_plural(attr)
        for a in attrs:
            try:
                return self.chunkable_attrs[self.get_index_from_list(a, self.chunkable_attrs) + 1]
            except (IndexError, KeyError, TypeError, ValueError):
                pass


    def needs_chunking(self, attr: str, data: Dict) -> bool:
        """
        Recursively analyses the data and identifies if the current level of data should be chunked.
        This could happen if either isolate_attr marker in the current scope or recursively in any of sub-elements.

        :param attr:    Name of attribute you want to check for chunking.
        :param data:    Input dictionary to analyse.
        """

        attrs = single_or_plural(attr)
        isolate_attrs = [f"isolate_{a}" for a in attrs]

        if any(data[x] for x in isolate_attrs if x in data):
            logger.debug(f"Got requirement to isolate {attr} in the current scope: {data}")
            return True

        next_attr = self.get_next_chunkable_attr(attr)

        logger.debug(f"Found next attr {next_attr}, for {attr} from {data}")
        # We are not yet lowest level going recursive
        if next_attr:
            for a in attrs:
                current_vals = get_list_of_multiple_or_one_or_empty_from_dict(data, a)
                logger.debug(f"For {a} got current_vals: {current_vals} from {data}. Analysing {next_attr}")

                for val in current_vals:

                    for name, subdata in val.items():
                        logger.debug(f"Analysing {next_attr} in {subdata}")
                        if not subdata:
                            continue
                        logger.debug(f"Going recursive for {next_attr} in {subdata}")
                        if self.needs_chunking(next_attr, subdata):
                            logger.debug(f"Returning True for {next_attr} from {subdata}")
                            return True

        return False


    def extract_job_from_payload(self, event: Dict):
        """ Parse and basically validate job from the event. """


        def load(obj):
            return obj if isinstance(obj, dict) else json.loads(obj)


        jh = load(event)
        job = load(jh['job']) if 'job' in jh else jh

        assert 'lambda_name' in job, f"Job is missing required parameter 'lambda_name': {job}"
        job['lambda_name'] = job['lambda_name']

        return job


    def get_name_from_arn(self, arn):
        """ Extract just the name of function from full ARN. Supports versions, aliases or raw name (without ARN). """

        pattern = "(arn:aws:lambda:[0-9a-zA-Z-]{6,12}:[0-9]{12}:function:)?" \
                  "(?P<name>[0-9a-zA-Z_=,.@-]*)(:)?([0-9a-zA-Z$]*)?"

        return re.search(pattern, arn).group('name')


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
                    self.task_client.create_task(labourer=NotImplemented, **json.loads(task))
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

            # If there is no dat remaining in the file we remove it.
            if os.path.getsize(file_name) == 0:
                os.remove(file_name)

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


    def get_and_lock_queue_file(self) -> str:
        """
        Either take a new (recently created) file in local /tmp/, or download the version of queue file from S3.
        We move the file in S3 to `locked_` by prefix state or simply upload the new one there in `locked_` state.

        :return: Local path to the file.
        """

        if not os.path.isfile(self._local_queue_file):
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

        # If the local file exists (means we have probably just created it). Then we upload it in `locked_` state.
        else:
            self.s3_client.upload_file(Filename=self._local_queue_file, Bucket=self._queue_bucket,
                                       Key=self._remote_queue_locked_file)
        return self._local_queue_file


    def upload_and_unlock_queue_file(self):
        """
        Upload the local queue file to S3 and remove the `locked_` by prefix copy if it exists.

        # TODO Should first validate that the `locked` belongs to you. Your should probably abandon everything if not.
        # TODO Otherwise your `_remote_queue_file` will likely get overwritten by someone.
        """

        # If there is data left unprocessed in the file, upload it for future processing by siblings or someone else.
        if os.path.isfile(self._local_queue_file):
            self.s3_client.upload_file(Filename=self._local_queue_file, Bucket=self._queue_bucket,
                                       Key=self._remote_queue_file)

        # Delete the locked file from S3 (aka unlock)
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
        # TODO should add some labourer id here and some job ID or something.
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
