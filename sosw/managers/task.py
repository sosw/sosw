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

__all__ = ['TaskManager']
__author__ = "Nikolay Grishchenko"
__version__ = "1.0"

import boto3
import json
import logging
import time
import uuid

from copy import deepcopy
from json.decoder import JSONDecodeError
from pkg_resources import parse_version
from typing import Dict, List, Optional, Union

from sosw.app import Processor
from sosw.components.benchmark import benchmark
from sosw.components.dynamo_db import DynamoDbClient
from sosw.components.helpers import first_or_none
from sosw.labourer import Labourer


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class TaskManager(Processor):
    """.. _task:

    TaskManager is the core class used by most ``sosw`` Lambdas.
    It handles all the operations with tasks thus the configuration of this Manager is essential during your ``sosw`` implementation.

    The default version of TaskManager works with DynamoDB tables to store and analyze the state of Tasks.
    This could be upgraded in future versions to work with other persistent storage or DBs.

    The very important concept to understand about Task workflow is `greenfield`. :ref:`Read more <greenfield>`.

    """

    DEFAULT_CONFIG = {
        'init_clients':                            ['DynamoDb', 'lambda', 'Ecology'],
        'dynamo_db_config':                        {
            'table_name':       'sosw_tasks',
            'index_greenfield': 'sosw_tasks_greenfield',
            'row_mapper':       {
                'task_id':             'S',
                'labourer_id':         'S',
                'created_at':          'N',
                'completed_at':        'N',
                'greenfield':          'N',
                'attempts':            'N',
                'closed_at':           'N',
                'desired_launch_time': 'N',
                'arn':                 'S',
                'payload':             'S'
            },
            'required_fields':  ['task_id', 'labourer_id', 'created_at', 'greenfield'],

            # You can overwrite field names to match your DB schema. But the types should be the same.
            # By default takes the key itself.
            'field_names':      {
                'task_id': 'task_id',  # This is just an example
            }
        },
        'sosw_closed_tasks_table':                 'sosw_closed_tasks',
        'sosw_closed_tasks_labourer_status_index': 'labourer_task_status_with_time',
        'sosw_retry_tasks_table':                  'sosw_retry_tasks',
        'sosw_retry_tasks_greenfield_index':       'labourer_id_greenfield',
        'greenfield_invocation_delta':             31557600,  # 1 year.
        'greenfield_task_step':                    1000,
        'labourers':                               {
            # 'some_function': {
            #     'arn':                          'arn:aws:lambda:us-west-2:0000000000:function:some_function',
            #     'max_simultaneous_invocations': 10,
            #     # Health metrics for this Labourer should be stored in a dictionary.
            #     'health_metrics':               {
            #     # Name of the metric is just for human readability (probaly some future GUI interfaces),
            #         'SomeDBCPU': {
            #             # The value must have ``'details'`` as a dict with kwargs for CloudWatch client.
            #             'details':                     {
            #                 'Name':       'CPUUtilization',
            #                 'Namespace':  'AWS/RDS',
            #                 'Period':     60,
            #                 'Statistics': ['Average'],
            #                 'Dimensions': [
            #                     {
            #                         'Name':  'DBInstanceIdentifier',
            #                         'Value': 'YOUR-DB'
            #                     },
            #                 ],
            #             },
            #
            #             # These is the mapping of how the Labourer should "feel" about this metric.
            #             # See EcologyManager.ECO_STATUSES.
            #             # This is just a mapping ``ECO_STATUS: value`` using ``feeling_comparison_operator``.
            #             'feelings':                    {
            #                 3: 50,
            #                 4: 25,
            #             },
            #             'feeling_comparison_operator': '<='
            #         },
            #     },
            # },
        },
        'max_attempts':                            3,
        'max_closed_to_analyse_for_duration':      10,
        'max_simultaneous_invocations':            1,
    }

    __labourers = None

    # these clients will be initialized by Processor constructor
    # ecology_client: EcologyManager = None
    ecology_client = None
    dynamo_db_client: DynamoDbClient = None
    lambda_client: boto3.client = None


    def get_oldest_greenfield_for_labourer(self, labourer: Labourer, reverse: bool = False) -> int:
        """
        Return value of oldest greenfield in queue.
        This means the beginning of the queue if you need FIFO behaviour.
        """

        _ = self.get_db_field_name

        q = dict(
                keys={_('labourer_id'): labourer.id, _('greenfield'): str(time.time())},
                comparisons={_('greenfield'): '<='},
                max_items=1,
                index_name=self.config['dynamo_db_config']['index_greenfield']
        )
        if reverse:
            q['desc'] = True

        items = self.dynamo_db_client.get_by_query(**q)

        if items:
            first_task_in_queue = items[0]
            result = int(first_task_in_queue[_('greenfield')])

        # Logically this is 0 (aka beginning of the Epoch), but we sometimes want to put earlier than the oldest task,
        # so let us assume we begin one step ahead of The zero.
        else:
            result = 0 + int(self.config['greenfield_task_step'])

        logger.debug(f"get_oldest_greenfield_for_labourer(reverse={reverse}) returned: {result}")
        return result


    def get_newest_greenfield_for_labourer(self, labourer: Labourer) -> int:
        """
        Return value of the newest greenfield in queue. This means the end of the queue or latest added.
        """
        return self.get_oldest_greenfield_for_labourer(labourer, reverse=True)


    def get_length_of_queue_for_labourer(self, labourer: Labourer) -> int:
        """
        Approximate count of tasks still in queue for `labourer`.
        Tasks with greenfield <= now()

        :param labourer:
        :return:
        """

        _ = self.get_db_field_name

        queue_count = self.dynamo_db_client.get_by_query(
                keys={_('labourer_id'): labourer.id, _('greenfield'): str(time.time())},
                comparisons={'greenfield': '<='},
                index_name=self.config['dynamo_db_config']['index_greenfield'],
                return_count=True)

        return queue_count


    def register_labourers(self) -> List[Labourer]:
        """
        Sets timestamps, health status and other custom attributes on Labourer objects passed for registration.

        We also send a pointer to the TaskManager (aka self) to Ecology Manager.
        The latter will have to make some queries, and we don't want him to initialise another TaskManager for himself.
        """

        _ = self.get_db_field_name
        _cfg = self.config.get

        self.ecology_client.register_task_manager(self)

        # WARNING! This must be something ordered, because these methods depend on one another.
        custom_attributes = (
            ('start', lambda x: int(time.time())),
            ('invoked', lambda x: x.get_attr('start') + self.config['greenfield_invocation_delta']),
            ('expired', lambda x: x.get_attr('invoked') - (x.duration + x.cooldown)),
            ('health', lambda x: self.ecology_client.get_labourer_status(x)),
            ('health_metrics', lambda x: _cfg('labourers')[x.id].get('health_metrics')) or {},
            ('max_attempts', lambda x: self.config.get(f'max_attempts_{x.id}') or self.config['max_attempts']),
            ('max_duration', lambda x: self.ecology_client.get_max_labourer_duration(x)),
            ('average_duration', lambda x: self.ecology_client.get_labourer_average_duration(x)),
            ('max_simultaneous_invocations', lambda x: _cfg('labourers')[x.id].get('max_simultaneous_invocations')
                                                       or _cfg('max_simultaneous_invocations')),
        )

        # Reset old Labourers and reconstruct them with fresh data.
        self.__labourers = None
        labourers = self.get_labourers()

        result = []
        for labourer in labourers:
            for k, method in [x for x in custom_attributes]:
                labourer.set_custom_attribute(k, method(labourer))
                logger.debug(f"SET for {labourer}: {k} = {method(labourer)}")
            result.append(labourer)

            for attr, val in _cfg('labourers')[labourer.id].items():
                labourer.set_custom_attribute(attr, val)

        self.__labourers = result

        return result


    def get_labourers(self) -> List[Labourer]:
        """
        Return configured Labourers.
        Config of the TaskManager expects 'labourers' as a dict 'name_of_lambda': {'some_setting': 'value1'}
        """

        if not self.__labourers:
            self.__labourers = [Labourer(id=name, **settings) for name, settings in self.config['labourers'].items()]

        return self.__labourers


    def get_labourer(self, labourer_id: str) -> Labourer:
        return first_or_none(self.get_labourers(), lambda x: x.id == labourer_id)


    def get_db_field_name(self, key: str) -> str:
        """ Could be useful if you overwrite field names with your own ones (e.g. for tests). """
        return self.config['dynamo_db_config']['field_names'].get(key, key)


    def create_task(self, labourer: Labourer, strict: bool = True, **kwargs):
        """
        Schedule a new task.

        :param labourer:    Labourer object of Lambda to execute the task.
        :param bool strict: By default (True) prohibits specifying in the task (kwargs) the fields that are supposed
                            to be autogenerated. Only if they match with autogen - then pass. You can override this
                            and pass custom task properties setting strict = False
        """

        _ = self.get_db_field_name

        # Save a copy of kwargs, because we are going to play with them.
        kw = deepcopy(kwargs)

        # Some common function we may need to generate default values.
        autogenerators = {
            _('task_id'):     lambda: str(uuid.uuid1().hex),
            _('labourer_id'): lambda: str(labourer.id),
            _('created_at'):  lambda: str(time.time()),
            _('greenfield'):  lambda: str(self.get_newest_greenfield_for_labourer(labourer)
                                          + int(self.config['greenfield_task_step'])),
            _('attempts'):    lambda: '0',
        }

        new_task = {}

        # Fill task fields from either kwargs or autogenerator.
        for key, gen in autogenerators.items():
            if strict:
                suggested = kw.pop(key, None)
                if suggested:
                    if str(suggested) != gen():
                        raise ValueError(f"Value of {key} passed to `create_task` doesnot match autogenerated. "
                                         f"Use strict = False if you really have to create a task like this.")
                else:
                    suggested = gen()

            else:
                suggested = str(kw.pop(key, gen()))
            new_task[key] = suggested

        if not all(key in new_task for key in self.config['dynamo_db_config'].get('required_fields', [])):
            raise ValueError(f"Required key {key} is missing in task {kwargs} and "
                             f"we don't have any auto generator for it.")

        try:
            new_task['payload'] = self.construct_payload_for_task(**kw)
        except Exception:
            raise ValueError(f"Unexpected `payload` or custom attrs for task '{kwargs}'. Should be dict() or JSON.")

        # Saving to DynamoDB.
        self.dynamo_db_client.put(new_task)
        logger.debug(f"Created a task: {new_task}")


    def construct_payload_for_task(self, **kwargs) -> str:
        """
        Combines remaining kwargs to a singular JSON payload.
        """

        _ = self.get_db_field_name

        # Now create and fill the payload
        payload = kwargs.pop(_('payload'), dict())

        # We have to use a dictionary to add more elements soon.
        if isinstance(payload, str):
            try:
                result = json.loads(payload)
            except Exception:
                result = dict(payload=payload)
        elif not isinstance(payload, dict):
            result = dict(payload=payload)
        else:
            result = payload

        # Add other fields from kwargs to payload.
        for key, value in kwargs.items():
            result[key] = value

        logger.debug(f"Constructed payload ({result})from {kwargs}")
        return json.dumps(result)


    def is_valid_task(self, task: Dict) -> bool:
        """
        Simple validation for required fields.
        """
        _ = self.get_db_field_name
        _cfg = self.config.get

        return all(field in task.keys() for field in [_('task_id'), _('labourer_id'), _('created_at')])


    def invoke_task(self, labourer: Labourer, task_id: Optional[str] = None, task: Optional[Dict] = None):
        """
        Invoke the Lambda Function execution for `task`.
        Providing the ID is more expensive, but safer from "task injection" attacks method that prefetches Task
        from the table before trying to invoke.

        Skips already running tasks with no exception, thus concurrent Orchestrators (or whoever else)
        should not duplicate invocations.
        """

        if not any([task, task_id]) or all([task, task_id]):
            raise AttributeError(f"You must provide any of `task` or `task_id`.")

        if not task:
            task = self.get_task_by_id(task_id=task_id)

        if not self.is_valid_task(task):
            raise ValueError(f"Task to invoke is invalid: {task}")

        try:
            self.mark_task_invoked(labourer, task)
        except Exception as err:
            if err.__class__.__name__ == 'ConditionalCheckFailedException':
                logger.warning(f"Update failed due to already running task {task}. "
                               f"Probably concurrent Orchestrator already invoked.")
                self.stats['concurrent_task_invocations_skipped'] += 1
                return
            else:
                logger.exception(err)
                raise RuntimeError(err)

        # Flatten the payload
        call_payload = task.pop('payload', {})
        if isinstance(call_payload, str):
            try:
                call_payload = json.loads(call_payload)
            except JSONDecodeError as err:
                logger.exception(f"Failed to decode payload: {call_payload}. Probably invalid task.")
                # TODO should panic, but not die here. Just skip the task.

        call_payload.update(task)

        # Support for asyncio does not exist for botocore right now. See ticket below:
        # https://github.com/boto/botocore/issues/458
        # There is another library that makes an async version of botocore, but doesn't
        # support lambda currently. See below:
        # https://github.com/aio-libs/aiobotocore
        lambda_response = self.lambda_client.invoke(
                FunctionName=labourer.arn,
                InvocationType='Event',
                Payload=json.dumps(call_payload)
        )
        logger.debug(lambda_response)

        self.stats['invoked_tasks'] += 1


    def mark_task_invoked(self, labourer: Labourer, task: Dict, check_running: Optional[bool] = True):
        """
        Update the greenfield with the latest invocation timestamp + invocation_delta

        By default updates with a conditional expression that fails in case the current greenfield is already in
        `invoked` state. If this check fails the function raises RuntimeError that should be handled
        by the Orchestrator. This is very important to help duplicate invocations of the Worker by simultaneously
        running Orchestrators.

        :param labourer:        Labourer for the task
        :param task:            Task dictionary
        :param check_running:   If True (default) updates with conditional expression.

        :raises RuntimeError

        """

        _ = self.get_db_field_name

        assert labourer.id == task[_('labourer_id')], f"Task doesn't belong to the Labourer {labourer}: {task}"

        self.dynamo_db_client.update(
                {_('task_id'): task[_('task_id')]},
                attributes_to_update={_('greenfield'): int(time.time()) + self.config['greenfield_invocation_delta']},
                attributes_to_increment={_('attempts'): 1},
                condition_expression=f"{_('greenfield')} < {labourer.get_attr('start')}"
        )


    # Deprecated
    # def close_task(self, task_id: str, labourer_id: str):
    #     _ = self.get_db_field_name
    #
    #     self.dynamo_db_client.update(
    #             {_('task_id'): task_id, _('labourer_id'): labourer_id},
    #             attributes_to_update={_('closed_at'): int(time.time())},
    #     )

    def archive_task(self, task_id: str):
        _ = self.get_db_field_name

        # Get task
        task = self.get_task_by_id(task_id)

        # Update labourer_id_task_status field.
        is_completed = 1 if task.get(_('completed_at')) else 0
        labourer_id = task.get(_('labourer_id'))
        task[_('labourer_id_task_status')] = f"{labourer_id}_{is_completed}"
        task[_('closed_at')] = int(time.time())

        # Add it to completed tasks table:
        self.dynamo_db_client.put(task, table_name=self.config.get('sosw_closed_tasks_table'))

        # Delete it from tasks_table
        keys = {_('task_id'): task[_('task_id')]}
        self.dynamo_db_client.delete(keys)

        self.stats['archived_tasks'] += 1


    def get_task_by_id(self, task_id: str) -> Dict:
        """ Fetches the full data of the Task. """

        tasks = self.dynamo_db_client.get_by_query({self.get_db_field_name('task_id'): task_id}, fetch_all_fields=True)
        assert len(tasks) in [0, 1], "Fetched more than 1 task by primary key(). Something broke your DB " \
                                     "schema."
        return tasks[0] if tasks else {}


    def get_next_for_labourer(self, labourer: Labourer, cnt: int = 1, only_ids: bool = False) -> List[Union[str, Dict]]:
        """
        Fetch the next task(s) from the queue for the Labourer.

        :param labourer:    Labourer to get next tasks for.
        :param cnt:         Optional number of Tasks to fetch.
        :param only_ids:    If explicitly set True, then returns only the IDs of tasks.
                            This could save some transport if you are sending big batches of tasks between Lambdas.

        """

        # Maximum value to identify the task as available for invocation (either new, or ready for retry).
        max_greenfield = labourer.get_attr('start')

        result = self.dynamo_db_client.get_by_query(
                {
                    self.get_db_field_name('labourer_id'): labourer.id,
                    self.get_db_field_name('greenfield'):  max_greenfield
                },
                table_name=self.config['dynamo_db_config']['table_name'],
                index_name=self.config['dynamo_db_config']['index_greenfield'],
                fetch_all_fields=False,
                max_items=cnt,
                comparisons={
                    self.get_db_field_name('greenfield'): '<'
                })

        logger.debug(f"get_next_for_labourer() received: {result} from {self.config['dynamo_db_config']['table_name']} "
                     f"for labourer: {labourer.id} max greenfield: {max_greenfield}")

        return result if not only_ids else [task[self.get_db_field_name('task_id')] for task in result]


    def get_invoked_tasks_for_labourer(self, labourer: Labourer, completed: Optional[bool] = None) -> List[Dict]:
        """
        Return a list of tasks of current Labourer invoked during the current run of the Orchestrator.

        If completed is provided:
        * True - filter completed ones
        * False - filter NOT completed ones
        * None (default) - do not care about `completed` status.
        """

        _ = self.get_db_field_name

        query_args = {
            'keys':        {
                _('labourer_id'): labourer.id,
                _('greenfield'):  labourer.get_attr('invoked')
            },
            'comparisons': {_('greenfield'): '>='},
            'index_name':  self.config['dynamo_db_config']['index_greenfield'],
        }

        if completed is True:
            query_args['filter_expression'] = f"attribute_exists {_('completed_at')}"
        elif completed is False:
            query_args['filter_expression'] = f"attribute_not_exists {_('completed_at')}"
        else:
            logger.debug(f"No filtering by completed status for {query_args}")

        return self.dynamo_db_client.get_by_query(**query_args)


    def get_running_tasks_for_labourer(self, labourer: Labourer, count: bool = False) -> Union[List[Dict], int]:
        """
        Return a list of tasks of Labourer previously invoked, but not yet closed or expired.
        We assume they are still running.

        If `count` is specified as True will return just the number of tasks, not the items themselves.
        Much cheaper.
        """

        _ = self.get_db_field_name

        q = dict(
                keys={
                    _('labourer_id'):                labourer.id,
                    f"st_between_{_('greenfield')}": labourer.get_attr('expired'),
                    f"en_between_{_('greenfield')}": labourer.get_attr('invoked'),
                },
                index_name=self.config['dynamo_db_config']['index_greenfield'],
                filter_expression=f'attribute_not_exists {_("completed_at")}'
        )

        if count:
            q['return_count'] = True

        return self.dynamo_db_client.get_by_query(**q)


    def get_count_of_running_tasks_for_labourer(self, labourer: Labourer) -> int:
        """
        Returns a number of tasks we assume to be still running.
        Theoretically they can be dead with Exception, but not yet expired.
        """

        return self.get_running_tasks_for_labourer(labourer=labourer, count=True)


    def get_completed_tasks_for_labourer(self, labourer: Labourer) -> List[Dict]:
        """
        Return a list of tasks of the Labourer marked as completed.
        Scavenger is supposed to archive them all so no special filtering is required here.

        In order to be able to use the already existing `index_greenfield`, we sort tasks only in invoked stages
        (`greenfield > now()`). This number is supposed to be small, so filtering by an un-indexed field will be fast.
        """

        _ = self.get_db_field_name

        query_args = {
            'keys':              {
                _('labourer_id'): labourer.id,
                _('greenfield'):  str(time.time()),
            },
            'comparisons':       {_('greenfield'): '>='},
            'index_name':        self.config['dynamo_db_config']['index_greenfield'],
            'filter_expression': f"attribute_exists {_('completed_at')}",
        }

        return self.dynamo_db_client.get_by_query(**query_args)


    def get_expired_tasks_for_labourer(self, labourer: Labourer) -> List[Dict]:
        """ Return a list of tasks of Labourer previously invoked, and expired without being closed. """

        _ = self.get_db_field_name

        return self.dynamo_db_client.get_by_query(
                keys={
                    _('labourer_id'):                labourer.id,
                    f"st_between_{_('greenfield')}": labourer.get_attr('start'),
                    f"en_between_{_('greenfield')}": labourer.get_attr('expired'),
                },
                index_name=self.config['dynamo_db_config']['index_greenfield'],
                filter_expression=f"attribute_not_exists {_('completed_at')}",
                fetch_all_fields=True
        )


    def _jsonify_payload_of_task(self, task: Dict) -> Dict:
        """
        Simple helper to make sure the `payload` of the `task` is a string. If it's a dict - JSONify it.
        :param dict task:
        """

        payload = task.get('payload')
        if isinstance(payload, dict):
            logger.debug(f"JSON-ify payload of the task: {task}")
            task['payload'] = json.dumps(payload)

        return task


    def move_task_to_retry_table(self, task: Dict, wanted_delay: int):
        """
        Put the task to a Dynamo table `sosw_retry_tasks`, with the wanted delay: labourer.max_runtime * attempts.
        Delete it from `sosw_tasks` table.
        """

        _ = self.get_db_field_name

        # Add task to retry table
        retry_row = task.copy()
        retry_row[_('desired_launch_time')] = int(time.time()) + wanted_delay
        retry_row = self._jsonify_payload_of_task(retry_row)

        self.dynamo_db_client.put(retry_row, table_name=self.config.get('sosw_retry_tasks_table'))

        # Delete task from tasks table
        delete_keys = {_('task_id'): task[_('task_id')]}
        self.dynamo_db_client.delete(delete_keys)

        self.stats['scheduled_for_retry_later_tasks'] += 1


    def get_tasks_to_retry_for_labourer(self, labourer: Labourer, limit: int = None) -> List[Dict]:
        _ = self.get_db_field_name

        attrs = {
            'keys':        {_('labourer_id'): labourer.id, _('desired_launch_time'): str(labourer.get_attr('start'))},
            'comparisons': {_('desired_launch_time'): "<="},
            'table_name':  self.config['sosw_retry_tasks_table'],
            'index_name':  self.config['sosw_retry_tasks_greenfield_index'],
        }
        if limit:
            attrs['max_items'] = limit
        tasks = self.dynamo_db_client.get_by_query(**attrs)
        return tasks


    def retry_tasks(self, labourer: Labourer, tasks: List[Dict]):
        """
        Move tasks to tasks table, in beginning of the queue (with greenfield of a task that will be invoked next)
        All tasks must belong to the same labourer.
        """

        _ = self.get_db_field_name

        for task in tasks:
            assert task[_('labourer_id')] == labourer.id, f"Task labourer_id must be {labourer.id}, " \
                f"bad value: {task[_('labourer_id')]}"

        lowest_greenfield = self.get_oldest_greenfield_for_labourer(labourer)

        for task in tasks:
            del task['desired_launch_time']
            lowest_greenfield = lowest_greenfield - 1
            task[_('greenfield')] = lowest_greenfield
            task = self._jsonify_payload_of_task(task)

            delete_keys = {_('labourer_id'): labourer.id, _('task_id'): task[_('task_id')]}

            # If boto supports DynamoDB transaction, use them to add task to tasks_table and delete from retry_table
            # https://github.com/boto/boto3/issues/1791: It's available for 1.9.54+
            if parse_version(str(boto3.__version__)) >= parse_version('1.9.54'):
                put_query = self.dynamo_db_client.make_put_transaction_item(task)
                delete_query = self.dynamo_db_client.make_delete_transaction_item(
                        delete_keys, table_name=self.config.get('sosw_retry_tasks_table'))
                self.dynamo_db_client.transact_write(put_query, delete_query)

            else:
                logger.info("Looks like you are running an ancient copy of boto3 still in old Environment of Lambda."
                            "Salut to AWS from March 2019.")
                self.dynamo_db_client.put(task)
                self.dynamo_db_client.delete(keys=delete_keys, table_name=self.config.get('sosw_retry_tasks_table'))

            self.stats['due_for_retry_tasks'] += 1


    @benchmark
    def get_average_labourer_duration(self, labourer: Labourer) -> int:
        """
        Analyse latest tasks of Labourer and calculate average runtime duration.

        .. warning:: This method doesn't know the exact duration of failed attempts.
                     Thus if the task is completely failed, we assume that all attempts failed at maximum duration.

        :return:    Average duration in seconds.
        """

        _ = self.get_db_field_name
        _cfg = self.config.get

        durations = []

        q = dict(
                keys={
                    _('labourer_id_task_status'): f"{labourer.id}_1",
                },
                table_name=_cfg('sosw_closed_tasks_table'),
                index_name=_cfg('sosw_closed_tasks_labourer_status_index'),
                max_items=_cfg('max_closed_to_analyse_for_duration'),
                desc=True
        )

        # Fetch last X closed tasks
        tasks = self.dynamo_db_client.get_by_query(**q)

        # Fetch failed tasks as well
        q['keys'][_('labourer_id_task_status')] = f"{labourer.id}_0"
        tasks.extend(self.dynamo_db_client.get_by_query(**q))

        # Now take the really last 50 ordered by greenfield (last invocation)
        tasks = sorted(tasks, key=lambda x: x.get(_('greenfield')))[:_cfg('max_closed_to_analyse_for_duration')]

        # Get their duration
        for task in tasks:
            # We assume duration of failed tasks to be maximum.
            if not task.get(_('completed_at')):
                durations.extend([labourer.get_attr('max_duration') for _ in range(int(task[_('attempts')]))])
            else:
                # Duration of completed tasks we calculate based on the value of last `greenfield` and `completed_at`
                durations.append(
                        task[_('completed_at')] - task[_('greenfield')] + _cfg('greenfield_invocation_delta'))

        # Return the average
        try:
            return round(sum(durations) / len(durations))
        except ZeroDivisionError:
            return 0
