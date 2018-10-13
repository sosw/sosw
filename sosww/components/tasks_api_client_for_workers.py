import boto3
import logging
import time
import os


__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "1.1"
__license__ = "MIT"
__status__ = "Production"

__all__ = ['close_task']

logger = logging.getLogger()



def close_task(event, table=None, hash_key='created_ms', range_key='task_id'):
    """
    Mark the task as completed in DynamoDB by updating the `completed_at` field with the current timestamp.

    .. :note::
       The current version asserts that Hash key is `Number` and Range key is `string`.
       Feel free to upgrade this for autodetection and make a PR.

    In case the Range key is set to None (doesn't exist in the event), we assume that table has only
    the Hash key and try to update using it.

    :param dict event:   The Payload Worker Lambda has received. This method tries to extract keys from it.

    :param str table:       Name of the table to close task at. Default: `abs_tasks_running`
    :param str hash_key:    Name of the Primary key (Hash key) in the table. Default: `created_ms`
    :param str range_key:   Name of the Sort key (Range key) in the table. Default: `task_id`
    """

    test = True if os.environ.get('STAGE') in ['test', 'autotest'] else False

    table_name = table or ('autotest_abs_tasks_running' if test else 'abs_tasks_running')

    client = boto3.client('dynamodb')
    hk, rk = str(event.get(hash_key)), str(event.get(range_key))

    assert hk.isnumeric(), f"Hash key in the current version must be numeric, but we got: {hk}"

    if hk and rk:
        response = client.update_item(TableName=table_name,
                                      Key={
                                          hash_key: {'N': hk},
                                          range_key:    {'S': rk}
                                      },
                                      UpdateExpression=" SET completed_at = :now",
                                      ExpressionAttributeValues={':now': {'N': str(time.time())}})
        logger.info(f"Marked task {hk}-{rk} as completed")

    elif hk:
        response = client.update_item(TableName=table_name,
                                      Key={
                                          hash_key: {'N': hk}
                                      },
                                      UpdateExpression=" SET completed_at = :now",
                                      ExpressionAttributeValues={':now': {'N': str(time.time())}})
        logger.info(f"Marked task {hk} as completed")

    else:
        logger.warning("No primary key for Tasks DB received. Someone other that adw_pull_controller invoked me.")
