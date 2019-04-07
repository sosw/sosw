import boto3
import logging
import time
import os


__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "1.3"
__license__ = "MIT"
__status__ = "Production"

__all__ = ['close_task']

logger = logging.getLogger()



def close_task(event, table=None, hash_key='task_id', range_key='labourer_id'):
    """
    Mark the task as completed in DynamoDB by updating the `completed_at` field with the current timestamp.
    This function doesn't rely on components.dynamo_db because it can be used by the Worker for his own tasks
    with his own configurations. And reinitialization of the client just for closing is too expensive.
    Raw boto3 call here.

    TODO This function is not really generic enough. Upgrade it one day.

    .. :note::
       The current version asserts that Hash key is `String`.
       Feel free to upgrade this for autodetection and make a PR.

    :param dict event:   The Payload Worker Lambda has received. This method tries to extract key from it.

    :param str table:       Name of the table to close task at. Default: `abs_tasks_running`
    :param str hash_key:    Name of the Primary key (Hash key) in the table. Default: `task_id`
    """

    test = True if os.environ.get('STAGE') in ['test', 'autotest'] else False
    table_name = table or ('autotest_sosw_tasks' if test else 'sosw_tasks')

    hk = str(event.get(hash_key))
    # rk = str(event.get(range_key, os.environ.get('AWS_LAMBDA_FUNCTION_NAME')))

    client = boto3.client('dynamodb')

    if hk:
        response = client.update_item(TableName=table_name,
                                      Key={hash_key: {'S': hk}},
                                      UpdateExpression=" SET completed_at = :now",
                                      ExpressionAttributeValues={':now': {'N': str(time.time())}})
        logger.info(f"Marked task {hk} as completed")
        logger.debug(f"DynamoDB response for marking task: {response}")

    else:
        logger.warning("No primary key for Tasks DB received. Someone other that adw_pull_controller invoked me.")
