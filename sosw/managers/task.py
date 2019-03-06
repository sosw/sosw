__all__ = ['TaskManager']
__author__ = "Nikolay Grishchenko"
__version__ = "1.0"

import boto3
import json
import logging
import os
import time

from collections import defaultdict
from typing import Dict, List, Optional

from sosw.components.benchmark import benchmark
from sosw.labourer import Labourer
from sosw.app import Processor


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class TaskManager(Processor):
    DEFAULT_CONFIG = {
        'init_clients':                ['DynamoDb', 'lambda'],
        'dynamo_db_config':            {
            'table_name':       'sosw_tasks',
            'index_greenfield': 'sosw_tasks_greenfield',
            'row_mapper':       {
                'task_id':      'S',
                'labourer_id':  'S',
                'created_at':   'N',
                'completed_at': 'N',
                'greenfield':   'N',
                'attempts':     'N',
            },
            'required_fields':  ['task_id', 'labourer_id', 'created_at', 'greenfield'],

            # You can overwrite field names to match your DB schema. But the types should be the same.
            # By default takes the key itself.
            'field_names':      {
                'task_id': 'task_id',
            }
        },
        'greenfield_invocation_delta': 31557600,  # 1 year.
    }


    def register_labourers(self, labourers: List[Labourer]):
        """ Sets timestamps on Labourer objects passed for registration. """

        # This must be something ordered, because these methods depend on one another.
        TIMES = (
            ('start', lambda x: int(time.time())),
            ('invoked', lambda x: x.get_timestamp('start') + self.config['greenfield_invocation_delta']),
            ('expired', lambda x: x.get_timestamp('invoked') - (x.duration + x.cooldown)),
        )

        result = []
        for labourer in labourers:
            for k, method in [x for x in TIMES]:
                labourer.set_timestamp(k, method(labourer))

            result.append(labourer)

        return result


    def get_db_field_name(self, key):
        """ Could be useful if you overwrite field names with your own ones (e.g. for tests). """
        return self.config['dynamo_db_config']['field_names'].get(key, key)


    def create_task(self, **kwargs):
        raise NotImplementedError


    def invoke_task(self, labourer: Labourer, task_id: Optional[str] = None, task: Optional[Dict] = None):
        """ Invoke the Lambda Function execution for `task` """

        if not any([task, task_id]) or all([task, task_id]):
            raise AttributeError(f"You must provide any of `task` or `task_id`.")

        task = self.get_task_by_id(task_id=task_id)

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


        lambda_response = self.lambda_client.invoke(
                FunctionName=labourer.arn,
                InvocationType='Event',
                Payload=task.get('payload')
        )
        logger.debug(lambda_response)



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

        tf = self.get_db_field_name('task_id')  # Main key field
        lf = self.get_db_field_name('labourer_id')  # Range key field
        gf = self.get_db_field_name('greenfield')
        af = self.get_db_field_name('attempts')

        assert labourer.id == task[lf], f"Task doesn't belong to the Labourer {labourer}: {task}"

        self.dynamo_db_client.update(
                {tf: task[tf], lf: labourer.id},
                attributes_to_update={gf: int(time.time()) + self.config['greenfield_invocation_delta']},
                attributes_to_increment={af: 1},
                condition_expression=f"{gf} < {labourer.get_timestamp('start')}"
        )


    def close_task(self, task_id: str):
        raise NotImplementedError


    def get_task_by_id(self, task_id: str) -> Dict:
        """ Fetches the full data of the Task. """

        return self.dynamo_db_client.get_by_query({self.get_db_field_name('task_id'): task_id})


    def get_next_for_labourer(self, labourer: Labourer, cnt: int = 1) -> List[str]:
        """
        Fetch the next task(s) from the queue for the Labourer.

        :param labourer:   Labourer to get next tasks for.
        :param cnt:        Optional number of Tasks to fetch.
        """

        # Maximum value to identify the task as available for invocation (either new, or ready for retry).
        max_greenfield = labourer.get_timestamp('start')

        result = self.dynamo_db_client.get_by_query(
                {
                    self.get_db_field_name('labourer_id'): labourer.id,
                    self.get_db_field_name('greenfield'):  max_greenfield
                },
                table_name=self.config['dynamo_db_config']['table_name'],
                index_name=self.config['dynamo_db_config']['index_greenfield'],
                strict=True,
                max_items=cnt,
                comparisons={
                    self.get_db_field_name('greenfield'): '<'
                })

        logger.info(f"get_next_for_labourer() received: {result} from {self.config['dynamo_db_config']['table_name']} "
                    f"for labourer: {labourer.id} max greenfield: {max_greenfield}")

        return [task[self.get_db_field_name('task_id')] for task in result]


    def calculate_count_of_running_tasks_for_labourer(self, labourer: Labourer) -> int:
        """
        Returns a number of tasks we assume to be still running.
        Theoretically they can be dead with Exception, but not yet expired.
        """

        return len(self.get_running_tasks_for_labourer(labourer=labourer))


    def get_invoked_tasks_for_labourer(self, labourer: Labourer, closed: Optional[bool] = None) -> List[Dict]:
        """
        Return a list of tasks of current Labourer invoked during the current run of the Orchestrator.

        If closed is provided:
        * True - filter closed ones
        * False - filter NOT closed ones
        * None (default) - do not care about `closed` status.
        """

        lf = self.get_db_field_name('labourer_id')
        gf = self.get_db_field_name('greenfield')

        query_args = {
            'keys':        {
                lf: labourer.id,
                gf: labourer.get_timestamp('invoked')
            },
            'comparisons': {gf: '>='},
            'index_name':  self.config['dynamo_db_config']['index_greenfield'],
        }

        if closed is True:
            query_args['filter_expression'] = 'attribute_exists closed'
        elif closed is False:
            query_args['filter_expression'] = 'attribute_not_exists closed'
        else:
            logger.debug(f"No filtering by closed status for {query_args}")

        return self.dynamo_db_client.get_by_query(**query_args)


    def get_running_tasks_for_labourer(self, labourer: Labourer) -> List[Dict]:
        """
        Return a list of tasks of Labourer previously invoked, but not yet closed or expired.
        We assume they are still running.
        """

        lf = self.get_db_field_name('labourer_id')
        gf = self.get_db_field_name('greenfield')

        return self.dynamo_db_client.get_by_query(
                keys={
                    lf:                 labourer.id,
                    f"st_between_{gf}": labourer.get_timestamp('expired'),
                    f"en_between_{gf}": labourer.get_timestamp('invoked'),
                },
                index_name=self.config['dynamo_db_config']['index_greenfield'],
                filter_expression='attribute_not_exists closed'
        )


    def get_closed_tasks_for_labourer(self, labourer: Labourer) -> List[Dict]:
        """
        Return a list of tasks of the Labourer marked as closed.
        Scavenger is supposed to archive them all so no special filtering is required here.

        In order to be able to use the already existing `index_greenfield`, we sort tasks only in invoked stages
        (`greenfield > now()`). This number is supposed to be small, so filtering by an un-indexed field will be fast.
        """

        return self.get_invoked_tasks_for_labourer(labourer=labourer, closed=True)


    def get_expired_tasks_for_labourer(self, labourer: Labourer) -> List[Dict]:
        """ Return a list of tasks of Labourer previously invoked, and expired without being closed. """

        lf = self.get_db_field_name('labourer_id')
        gf = self.get_db_field_name('greenfield')

        return self.dynamo_db_client.get_by_query(
                keys={
                    lf:                 labourer.id,
                    f"st_between_{gf}": labourer.get_timestamp('start'),
                    f"en_between_{gf}": labourer.get_timestamp('expired'),
                },
                index_name=self.config['dynamo_db_config']['index_greenfield'],
                filter_expression='attribute_not_exists closed',
        )
