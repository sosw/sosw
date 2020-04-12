"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - Serverless Orchestrator of Serverless Workers

    The MIT License (MIT)
    Copyright (C) 2019  sosw core contributors <info@sosw.app>

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
"""

__all__ = ['Scheduler']
__author__ = "Nikolay Grishchenko"
__version__ = "1.0"

import datetime
import json
import logging
import os
import re
import time

from collections import Iterable
from copy import deepcopy
from typing import List, Set, Tuple, Union, Optional, Dict

from sosw.essential import Essential
from sosw.app import LambdaGlobals
from sosw.components.helpers import get_list_of_multiple_or_one_or_empty_from_dict, trim_arn_to_name, chunks
from sosw.components.siblings import SiblingsManager
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


class Scheduler(Essential):
    """
    Scheduler is converting business jobs to one or multiple Worker tasks.

    Job supports a lot of dynamic settings that will coordinate the chunking.

    Parameters:

    `max_YOURATTRs_per_batch`: int

    This is applicable only to the lowest level of chunking.  If isolation of this parameters is not required, and all
    values of this parameter are simple strings/integers the scheduler shall chunk them in batches of given size.
    By default will chunk to 1kk objects in a list.

    """

    DEFAULT_CONFIG = {
        'init_clients':    ['Task', 's3', 'Sns', 'Siblings'],
        'task_config':     {
            'labourers': {
                # 'some_function': {
                #     'arn': 'arn:aws:lambda:us-west-2:000000000000:function:some_function',
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
            ],
        }
    }

    # these clients will be initialized by Processor constructor
    task_client: TaskManager = None
    siblings_client: SiblingsManager = None
    s3_client = None
    sns_client = None
    base_query = ...


    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.set_queue_file()

        self.chunkable_attrs = list([x[0] for x in self.config['job_schema']['chunkable_attrs']])
        assert not any(x.endswith('s') for x in self.chunkable_attrs), \
            f"We do not currently support attributes that end with 's'. " \
                f"In the config you should use singular form of attribute. Received from config: {self.chunkable_attrs}"


    def __call__(self, event):
        """
        Process an event.

        :param dict event: event data
        """

        job = self.extract_job_from_payload(event)

        # If called as sibling
        if 'file_name' in job:
            self.set_queue_file(job['file_name'])

        # else construct new data file
        else:
            self.parse_job_to_file(job)

        self.process_file()

        super().__call__(event)


    def parse_job_to_file(self, job: Dict):
        """
        Splits the Job to multiple tasks and writes them down in self.local_queue_file.

        :param dict job:    Payload from Scheduled Rule.
                            Should be already parsed from whatever payload to dict and contain the raw `job`
        """

        if os.path.isfile(self.local_queue_file):
            logger.critical(f"The current Lambda container is already having some unprocessed file. "
                            f"You probably did not clean it correctly after processing. "
                            f"Should probably just clean the file now, but during beta version we raise an stop "
                            f"processing new ones.")
            raise RuntimeError(f"The current Lambda container is already having some unprocessed file.")

        labourer = self.task_client.get_labourer(labourer_id=job.pop('lambda_name'))
        if not labourer:
            raise RuntimeError(f"Invalid (unregistered) Labourer: {labourer}. "
                               f"Maybe your job is missing `lambda_name`, or the one provided is not registered "
                               f"in the config of the Scheduler. Current job: {job}")

        # In case there is not chunking required, we just schedule `task` directly from the `job`.
        if not all([self.chunkable_attrs, self.needs_chunking(plural(self.chunkable_attrs[0]), job)]):
            data = [{'labourer_id': labourer.id, **job}]

        # Else there is much more logic how to chunk the job to tasks.
        else:
            data = self.construct_job_data(job, skeleton={'labourer_id': labourer.id})

        with open(self.local_queue_file, 'w') as f:
            for row in data:
                f.write(f"{json.dumps(row)}\n")

        logger.info(f"Finished step: parse_job_to_file()")


    # def create_tasks(self, labourer: Labourer, data: List):
    #     """
    #     Iterate tasks from `data` and queue them as new tasks for `labourer`.
    #     """
    #
    #     for task in data:
    #         self.task_client.create_task(labourer=labourer, payload=task)

    def validate_list_of_vals(self, data: Union[list, set, tuple, Dict]) -> list:
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
            if all(v is None for v in data.values()):
                return list(data.keys())

            elif len(data.keys()) == 1:
                return [data]

            else:
                raise InvalidJob(f"Job have values with embedded data, but chunking is not requested. "
                                 f"You should either append 'isolate_ATTRIBUTE' flag or make values appropriate. "
                                 f"Should be a flat list or a dict with None - values. Your job was: {data}")

        else:
            raise InvalidJob(f"A Job without chunking enabled should have value either as a simple iterable "
                             f"(list, set, tuple), or a dict with all the values = None."
                             f"You provided {type(data)}: {data}")


    def last_x_days(self, pattern: str) -> List[str]:
        """
        Constructs the list of date strings for chunking.
        """

        assert re.match('last_[0-9]+_days', pattern) is not None, "Invalid pattern {pattern} for `last_x_days()`"

        num = int(pattern.split('_')[1])
        today = datetime.date.today()

        return [str(today - datetime.timedelta(days=x)) for x in range(num, 0, -1)]


    def previous_x_days(self, pattern: str) -> List[str]:
        """
        Returns a list of string dates from today - x - x

        For example, consider today's date as 2019-04-30.
        If I call for previous_x_days(pattern='previous_2_days'), I will receive a list of string dates equal to:
        ['2019-04-26', '2019-04-27']
        """
        assert re.match('previous_[0-9]+_days', pattern) is not None, "Invalid pattern {pattern} for `previous_x_days`"

        num = int(pattern.split('_')[1])
        today = datetime.date.today()
        end_date = today - datetime.timedelta(days=num)

        return [str(end_date - datetime.timedelta(days=x)) for x in range(num, 0, -1)]


    def x_days_back(self, pattern: str) -> List[str]:
        """
        Finds the exact date X days back from now.
        Returns it as a `str` in a `list` following the interface requirements of `chunk_dates`.

        e.g. `1_days_back` - yesterday, `7_days_back` - same day as today last week
        """

        assert re.match('[0-9]+_days_back', pattern) is not None, "Invalid pattern {pattern} for `x_days_back()`"

        num = int(pattern.split('_')[0])
        today = datetime.date.today()

        return [str(today - datetime.timedelta(days=num))]


    def yesterday(self, pattern: str = 'yesterday') -> List[str]:
        """
        Simple wrapper for x_days_back() to return yesterday's date.
        """
        assert re.match('yesterday', pattern) is not None, "Invalid pattern {pattern} for `yesterday()`"
        return self.x_days_back('1_days_back')


    def today(self, pattern: str = 'today') -> List[str]:
        """
        Returns list with one datetime string (YYYY-MM-DD) equal to today's date.
        """
        assert re.match('today', pattern) is not None, "Invalid pattern {pattern} for `today()`"
        return [str(datetime.date.today())]


    def last_week(self, pattern: str = 'last_week') -> List[str]:
        """
        Returns list of dates (YYYY-MM-DD) as strings for last week (Sunday - Saturday)
        :param pattern:
        :return:
        """
        assert re.match('last_week', pattern) is not None, "Invalid pattern {pattern} for `last_week()`"

        today = datetime.date.today()
        end_date = today - datetime.timedelta(days=today.weekday() + 8)

        return [str(end_date + datetime.timedelta(days=x)) for x in range(7)]


    def chunk_dates(self, job: Dict, skeleton: Dict = None) -> List[Dict]:
        """
        There is a support for multiple not nested parameters to chunk. Dates is one very specific of them.
        """

        data = []
        skeleton = deepcopy(skeleton) or {}
        job = deepcopy(job)

        period = job.pop('period', None)
        isolate = job.pop('isolate_days', None)

        PERIOD_KEYS = ['last_[0-9]+_days', '[0-9]+_days_back', 'yesterday', 'today', 'previous_[0-9]+_days',
                       'last_week']

        if period:

            date_list = []
            for pattern in PERIOD_KEYS:
                if re.match(pattern, period):
                    # Call the appropriate method with given value from job.
                    logger.debug(f"Found period '{period}' for job {job}")
                    method_name = pattern.replace('[0-9]+', 'x', 1)
                    date_list = getattr(self, method_name)(period)
                    break
            else:
                raise ValueError(f"Unsupported period requested: {period}. Valid options are: "
                                 f"'last_X_days', 'X_days_back', 'yesterday', 'today', 'previous_[0-9]+_days', 'last_week'")

            if isolate:
                assert len(date_list) > 0, f"The chunking period: {period} did not generate date_list. Bad."

                for d in date_list:
                    data.append({**job, **skeleton, 'date_list': [d]})
            else:
                if len(date_list) > 1:
                    logger.debug("Running chunking for multiple days, but without date isolation. "
                                 "Your workers might feel bad.")
                data.append({**job, **skeleton, 'date_list': date_list})

        else:
            logger.debug(f"No `period` chunking requested in job {job}")
            data.append({**job, **skeleton})

        return data


    def construct_job_data(self, job: Dict, skeleton: Dict = None) -> List[Dict]:
        """
        Chunks the job to tasks using several layers. Each layer is represented with a `chunker` method.
        All chunkers should accept `job` and optional `skeleton` for tasks and return a list of tasks.
        If there is nothing to chunk for some chunker, return same `job` (with injected `skeleton`) wrapped in a list.

        Default chunkers:

        - Date list chunking
        - Recursive chunking for `chunkable_attrs`

        """

        CHUNKERS = [self.chunk_dates, self.chunk_job]

        data = [job]
        skeleton = deepcopy(skeleton) or {}

        for chunker in CHUNKERS:
            chunked = []  # Container for results of current chunker method.
            for task in data:
                logging.debug(f"Chunking {task} with {chunker}")
                chunked.extend(chunker(job=task))

            data = deepcopy(chunked)

        # Inject the skeleton to the resulting tasks
        for task in data:
            task.update(skeleton)

        return data


    def chunk_job(self, job: dict, skeleton: Dict = None, attr: str = None) -> List[Dict]:
        """
        Recursively parses a job, validates everything and chunks to simple tasks what should be chunked.
        The Scenario of chunking and isolation is worth another story, so you should put a link here once it is ready.
        """

        data = []
        skeleton = deepcopy(skeleton) or {}
        job = deepcopy(job)

        # The current attribute we are looking for in this iteration or the first one of preconfigured chunkables.
        attr = attr or self.chunkable_attrs[0] if self.chunkable_attrs else None

        # We have to return here the full job to let it work correctly with recursive calls.
        if not attr:
            return [{**job, **skeleton}]

        # If we shall need batching of flat vals of this attr we find out the batch size.
        # First we search in job (means the current level of recursive subdata being chunked.
        # If not specified per job, we try the setting inherited from level(s) upper probably even the root of main job.
        MAX_BATCH = 1000000  # This is not configurable!
        batch_size = int(job.get(f'max_{plural(attr)}_per_batch',
                             skeleton.get(f'max_{plural(attr)}_per_batch', MAX_BATCH)))


        def push_list_chunks():
            """ Appends chunks of lists using current skeleton and vals to chunk. """
            for v in chunks(vals, batch_size):
                data.append({**task_skeleton, **{plural(attr): v}})


        logger.debug(f"Testing for chunking {attr} from {job} with skeleton {skeleton}")
        # First of all decide whether we need to chunk current job (or a sub-job if called recursively).
        if self.needs_chunking(plural(attr), job):

            # Force batches to isolate if we shall be dealing with flat data.
            # But we still respect the `max_PARAM_per_batch` if it is provided in job.
            # Having batch_size == MAX_BATCH asserts that we had
            batch_size = 1 if batch_size == MAX_BATCH else batch_size

            # Next attribute is either name of attribute according to config, or None if we are already in last level.
            next_attr = self.get_next_chunkable_attr(attr)
            logger.debug(f"Next attr: {next_attr}")

            # Here and many places further we support both single and plural versions of attribute names.
            for possible_attr in single_or_plural(attr):
                logger.debug(f"Iterating possible: {possible_attr}")
                current_vals = get_list_of_multiple_or_one_or_empty_from_dict(job, possible_attr)
                if not current_vals:
                    continue

                # This is not the `skeleton` received during the call, but the remaining parts of the `job`,
                # not related to current `attr`
                job_skeleton = {k: v for k, v in job.items() if k not in [possible_attr, f"isolate_{plural(attr)}"]}
                logger.debug(f"For {possible_attr} we got current_vals: {current_vals} from {job}, "
                            f"leaving job_skeleton: {job_skeleton}")

                task_skeleton = {**deepcopy(skeleton), **job_skeleton}

                # For dictionaries we have to either go deeper recursively, or just flatten keys if values are None-s.
                if all(isinstance(v, dict) for v in current_vals):
                    for val in current_vals:

                        if all(x is None for x in val.values()):
                            logger.debug(f"Value {val} is all a dict of Nones. Need to flatten")
                            vals = self.validate_list_of_vals(val)
                            push_list_chunks()

                        else:
                            logger.debug(f"Real dictionary with values. Can't flatten it to dict: {val}")
                            for name, subdata in val.items():
                                logger.debug(f"SubIterating `{name}` with {subdata}")

                                # Merge parts of task
                                task = {**deepcopy(task_skeleton), **{plural(attr): [name]}}
                                logger.debug(f"Task sample: {task}")

                                if isinstance(subdata, dict):
                                    if not next_attr:
                                        # If there is no lower level configured to chunk, just keep this subdata in payload
                                        task.update(subdata)
                                        data.append(task)
                                    else:
                                        logger.debug(f"Call recursive for {next_attr} from subdata: {subdata}")
                                        data.extend(self.chunk_job(job=subdata, skeleton=task, attr=next_attr))

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
                    push_list_chunks()

        else:
            logger.debug(f"No need for chunking for attr: {attr} in job: {job}. Current skeleton is: {skeleton}")
            task_skeleton = {**deepcopy(skeleton)}
            for a in single_or_plural(attr):
                if a in job:
                    attr_value = job.pop(a, None)
                    if attr_value:
                        try:
                            vals = self.validate_list_of_vals(attr_value)
                            push_list_chunks()

                            # We are done here for not-chunkable attr. Return now.
                            return data

                        except InvalidJob:
                            logger.warning(f"Caught InvalidJob exception.")
                            # If a custom payload is not following the chunking convention - just translate it as is.
                            # And return the pop-ed value back to the job.
                            job[a] = attr_value
                        break
            else:
                logger.error(f"Did not find values for {attr} in job: {job}")
            # Populate the remaining parts of the job back to task.
            task_skeleton.update(job)
            data.append(task_skeleton)

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
        isolate_attrs = [f"isolate_{a}" for a in attrs] + [f"max_{a}_per_batch" for a in attrs]

        if any(data[x] for x in isolate_attrs if x in data):
            logger.debug(f"needs_chunking(): Got requirement to isolate {attr} in the current scope: {data}")
            return True

        next_attr = self.get_next_chunkable_attr(attr)

        logger.debug(f"needs_chunking(): Found next attr {next_attr}, for {attr} from {data}")
        # We are not yet lowest level going recursive
        if next_attr:
            for a in attrs:
                current_vals = get_list_of_multiple_or_one_or_empty_from_dict(data, a)
                logger.debug(f"needs_chunking(): For {a} got current_vals: {current_vals} from {data}. "
                             f"Analysing {next_attr}")

                for val in current_vals:

                    for name, subdata in val.items():
                        logger.debug(f"needs_chunking(): Analysing {next_attr} in {subdata}")
                        if not subdata:
                            continue
                        logger.debug(f"needs_chunking(): Going recursive for {next_attr} in {subdata}")
                        if self.needs_chunking(next_attr, subdata):
                            logger.debug(f"needs_chunking(): Returning True for {next_attr} from {subdata}")
                            return True

        return False


    def extract_job_from_payload(self, event: Dict):
        """ Parse and basically validate job from the event. """


        def load(obj):
            return obj if isinstance(obj, dict) else json.loads(obj)


        jh = load(event)
        job = load(jh['job']) if 'job' in jh else jh

        assert 'lambda_name' in job, f"Job is missing required parameter 'lambda_name': {job}"
        job['lambda_name'] = trim_arn_to_name(job['lambda_name'])

        return job


    def process_file(self):
        """
        Process a file for creating tasks, then uploading it to S3.
        In case of execution time reached its limit, spawning a new sibling to continue the processing.

        """

        file_name = self.get_and_lock_queue_file()

        if not file_name:
            logger.info(f"No file in queue.")
            return

        else:
            logger.info(f"Processing a file: {file_name}")
            while self.sufficient_execution_time_left:
                logger.debug(f"Execution time left: {global_vars.lambda_context.get_remaining_time_in_millis()}ms "
                            f"Working next batch of {self._rows_to_process} tasks from file {file_name}")
                data = self.pop_rows_from_file(file_name, rows=self._rows_to_process)
                if not data:
                    logger.info(f"No rows in file: {file_name}")
                    break

                for task in data:
                    logger.debug(f"Pushing task to DynamoDB: {task}")
                    t = json.loads(task)
                    labourer = self.task_client.get_labourer(t['labourer_id'])
                    self.task_client.create_task(labourer=labourer, **t)
                    time.sleep(self._sleeptime_for_dynamo)

            else:
                # Spawning another sibling to continue the processing
                logger.info(f"Ran out of execution time in `process_file`. Spawning sibling.")
                try:
                    payload = dict(file_name=file_name)
                    self.siblings_client.spawn_sibling(global_vars.lambda_context, payload=payload)
                    self.stats['siblings_spawned'] += 1

                except Exception as err:
                    logger.exception(
                        f"Could not spawn sibling with context: {global_vars.lambda_context}, payload: {payload}")

            self.upload_and_unlock_queue_file()
            self.clean_tmp()


    @property
    def _sleeptime_for_dynamo(self):
        """
        Pull DynamoDB write capcity dynamically to throttle at appropriate levels

        Calculates based on the assumption that a single write action consumes a full WCU
        Therefore multiple capacity units are calculated as a fraction of the
        """
        logging.debug(dir(self.task_client.dynamo_db_client))
        return 1 / self.task_client.dynamo_db_client.get_capacity()['write']


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


    def clean_tmp(self, file_name=None):
        file_to_remove = file_name or self.local_queue_file

        if os.path.isfile(file_to_remove):
            os.remove(file_to_remove)


    @property
    def _rows_to_process(self):
        return self.config['rows_to_process']


    @property
    def sufficient_execution_time_left(self) -> bool:
        """
        Return if there is a sufficient execution time for processing ('shutdown period' is in seconds).
        """

        return global_vars.lambda_context.get_remaining_time_in_millis() > self.config['shutdown_period'] * 1000


    def get_and_lock_queue_file(self) -> str:
        """
        Either take a new (recently created) file in local /tmp/, or download the version of queue file from S3.
        We move the file in S3 to `locked_` by prefix state or simply upload the new one there in `locked_` state.

        :return: Local path to the file.
        """

        if not os.path.isfile(self.local_queue_file):
            try:
                self.s3_client.download_file(Bucket=self._queue_bucket, Key=self.remote_queue_file,
                                             Filename=self.local_queue_file)
            except self.s3_client.exceptions.ClientError:
                self.stats['non_existing_remote_queue'] += 1
                logger.exception(f"Not found remote file to download")

            else:
                self.s3_client.copy_object(Bucket=self._queue_bucket,
                                           CopySource=f"{self._queue_bucket}/{self.remote_queue_file}",
                                           Key=self.remote_queue_locked_file)

                self.s3_client.delete_object(Bucket=self._queue_bucket, Key=self.remote_queue_file)

                logger.debug(f"Downloaded a copy of {self.local_queue_file} for processing "
                             f"and moved the remote one to {self.remote_queue_locked_file}.")

        # If the local file exists (means we have probably just created it). Then we upload it in `locked_` state.
        else:
            self.s3_client.upload_file(Filename=self.local_queue_file, Bucket=self._queue_bucket,
                                       Key=self.remote_queue_locked_file)

        return self.local_queue_file


    def upload_and_unlock_queue_file(self):
        """
        Upload the local queue file to S3 and remove the `locked_` by prefix copy if it exists.
        """

        # If there is data left unprocessed in the file, upload it for future processing by siblings or someone else.
        if os.path.isfile(self.local_queue_file):
            self.s3_client.upload_file(Filename=self.local_queue_file, Bucket=self._queue_bucket,
                                       Key=self.remote_queue_file)

        # Delete the locked file from S3 (aka unlock)
        try:
            self.s3_client.delete_object(Bucket=self._queue_bucket, Key=self.remote_queue_locked_file)
        except self.s3_client.exceptions.ClientError:
            logger.debug(f"No remote locked file to remove: {self.remote_queue_locked_file}. This is probably new.")


    @property
    def _queue_bucket(self):
        """ Name of S3 bucket for file with queue of tasks not yet in DynamoDB. """
        return self.config['queue_bucket']


    def set_queue_file(self, name: str = None):
        """
        Initialize a unique file_name to store the queue of tasks to write.
        """

        if name is None:
            filename_parts = self.config['queue_file'].rsplit('.', 1)
            assert len(filename_parts) == 2, "Got bad file name"
            self._queue_file_name = \
                f"{filename_parts[0]}_{global_vars.lambda_context.aws_request_id}.{filename_parts[1]}"
        else:
            self._queue_file_name = name


    @property
    def local_queue_file(self):
        return f"/tmp/{self._queue_file_name}"


    @property
    def remote_queue_file(self):
        """ Full S3 Key of file with queue of tasks not yet in DynamoDB. """
        return f"{self.config['s3_prefix'].strip('/')}/{self._queue_file_name}"


    @property
    def remote_queue_locked_file(self):
        """
        Full S3 Key of file with queue of tasks not yet in DynamoDB in the `locked` state.
        Concurrent processes should not touch it.
        """
        return f"{self.config['s3_prefix'].strip('/')}/locked_{self._queue_file_name}"


global_vars = LambdaGlobals()
