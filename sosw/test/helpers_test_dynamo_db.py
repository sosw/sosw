""" A set of helper functions for DynamoDB Integration testing. """

__all__ = ['AutotestDdbManager', 'get_autotest_ddb_suffix', 'get_autotest_ddb_name',
           'get_autotest_ddb_name_with_custom_suffix', 'get_autotest_ddb_config', 'get_ddb_benchmark',
           'clean_dynamo_table', 'create_test_ddb', 'drop_test_ddb', 'safe_put_to_ddb', 'get_table_setup', 'add_gsi']

__author__ = 'Nikolay Grishchenko'

import asyncio

import boto3
import logging
import time
import uuid

from collections import defaultdict
from copy import deepcopy
from typing import Dict, Optional, List, Tuple
from sosw.components.dynamo_db import clean_dynamo_table

TEST_DB_NAME = None
TEST_DB_SUFFIX = None
KEYS_OF_TABLES = defaultdict(dict)

DDB_CLIENT_CONFIG = {
    'row_mapper': {
        'hash_col': 'S',
        'range_col': 'M',
        'other_col': 'S',
    },
    'table_name': None,
    'required_fields': ['hash_col', 'range_col'],
    'key_fields': ['hash_col', 'range_col']
}

BENCHMARK = defaultdict(float)


def get_autotest_ddb_suffix():
    """ Get unique suffix for autotest tables for this run. """

    global TEST_DB_SUFFIX

    if TEST_DB_SUFFIX is None:
        TEST_DB_SUFFIX = str(uuid.uuid4())[:18]

    return TEST_DB_SUFFIX


def get_autotest_ddb_name():
    """ Get unique autotest db name. """

    global TEST_DB_NAME

    if TEST_DB_NAME is None:
        TEST_DB_NAME = f"autotest_{get_autotest_ddb_suffix()}"

    return TEST_DB_NAME


def get_autotest_ddb_name_with_custom_suffix(suffix: str) -> str:
    """
    Get unique DDB name with a provided suffix.
    Can be useful in cases when you need to create multiple DDB tables in your test environment.
    """

    return get_autotest_ddb_name() + '_' + suffix


def get_autotest_ddb_config():
    """
    Return a generic config for sosw DynamoDbClient for the autotest table: `row mapping`, `table_name`, etc.
    This might be helpful only in case you are using the default autotest_db for your integration tests.

    Example of usage:

    ..  code-block:: python

        def setUp(self) -> None:
            CONFIG = self.TEST_CONFIG.copy()
            CONFIG['dynamo_db_config'] = get_autotest_ddb_config()

            self.processor = Processor(custom_config=CONFIG)

    """

    result = DDB_CLIENT_CONFIG.copy()
    result['table_name'] = get_autotest_ddb_name()

    return result


def create_test_ddb(table_structure: Dict = None):
    """
    Creates a table with unique DB name using either sample generic schema or from argument.

    The function may also create Global Secondary Index (GSI) from table_structure.
    This significantly increases the creation time.

    :param Dict table_structure: Should follow the boto3 create_table() request specification.
    """

    global BENCHMARK

    st = time.perf_counter()
    client = boto3.client('dynamodb')

    if not table_structure.get('TableName'):
        table_structure['TableName'] = get_autotest_ddb_name()

    name = table_structure['TableName']

    assert name.startswith('autotest_'), f"Table for testing must start with an autotest prefix"

    logging.info(f"Creating table: {table_structure}")
    # Try to send the request to create a table. If already exists - first drop it and then recreate.
    for retry in range(2):
        try:
            client.create_table(**table_structure)
            logging.info(f'Successfully sent request to create table {name}')
            break

        except Exception as err:
            if "Table already exists" in str(err):
                for retry2 in range(2):
                    try:
                        drop_test_ddb(table_name=name)

                    except Exception as err2:
                        if "Table is being created" in str(err2):
                            time.sleep(retry2)

                        else:
                            raise Exception(err)

            else:
                logging.exception(err, exc_info=True)
                raise Exception(err)

    for i in range(34):  # ~10 minutes with exponential backoff
        response = client.describe_table(TableName=name)
        if response['Table']['TableStatus'] == 'ACTIVE':
            logging.info(f'Successfully created table {name} after {i} iterations.')
            break
        time.sleep(i)

    BENCHMARK['creating_table'] += time.perf_counter() - st

    return name


def drop_test_ddb(table_name: str = None):
    """ Drop temporary autotest DynamoDB table. """

    global BENCHMARK

    drop_st = time.perf_counter()

    name = table_name or get_autotest_ddb_name()

    assert name.startswith('autotest_'), f"Table for testing must start with an autotest prefix"

    client = boto3.client('dynamodb')

    for retry in range(3):
        try:
            client.delete_table(TableName=name)
            break

        except Exception as err2:
            if "Attempt to change a resource which is still in use" in str(err2):
                time.sleep(retry)
            else:
                raise Exception(err2)

    BENCHMARK['deleting_table'] += time.perf_counter() - drop_st

    for i in range(34):  # ~10 minutes with exponential backoff
        try:
            response = client.describe_table(TableName=name)
            logging.info(f"Table {name} is still {response['Table']['TableStatus']}")
            time.sleep(i)
        except:
            logging.info(f'Successfully dropped table {name} after {i} iterations.')
            break


def get_table_keys(table_name, client=None) -> List[str]:
    """
    Returns hash (and range if exists) keys of a table using describe_table with local cache.
    Uses either local boto3 client or the one passed from caller. Some extra costs, but only once per table.

    Cache saves keys as a dict for future use, although the function returns just the keys as a list.
    """

    global BENCHMARK, KEYS_OF_TABLES

    assert type(client).__name__ == "DynamoDB", \
        f"`client` argument if passed for `get_table_keys()` to must be a raw boto3 DynamoDB client."

    st = time.perf_counter()

    if table_name not in KEYS_OF_TABLES:
        table_description = client.describe_table(TableName=table_name)

        for attr in table_description['Table']['KeySchema']:
            KEYS_OF_TABLES[table_name][attr['AttributeName']] = attr['AttributeName']
        BENCHMARK['get_table_keys'] += time.perf_counter() - st

    return list(KEYS_OF_TABLES[table_name].values())


def safe_put_to_ddb(row: Dict, client, table_name: Optional[str] = None):
    """
    Writes the row to DynamoDB table and waits a little attempting to minimise eventual consistency problem.
    It actually waits till it can read the object from the DDB itself, although this doesn't eliminate the problem,
    just minimise it. The only approved use-case is when you need to write some SAMPLE DATA to the table and then
    test the Processor code to read it and do something. Usually Processor methods should not use Consistent Read
    themselves unless required by the business workflow.

    ..  warning:: Should be used in tests setup only!

    :param row:         Single row of data to write
    :param client:      sosw DynamoDbClient initialised in your test. You have to pass it here, to make sure that same
                        config is used and avoid unnecessary initialisations.
    :param table_name:  Optional name of the table. By default, the 'table_name' from your client configuration.
    """

    global BENCHMARK, KEYS_OF_TABLES

    st = time.perf_counter()

    client.put(row=row, table_name=table_name)

    table_name = table_name or client.config['table_name']
    keys = get_table_keys(table_name=table_name, client=client.dynamo_client)
    query = {k: row[k] for k in keys}

    for i in range(1, 10):
        time.sleep(i * 0.2)
        if client.get_by_query(keys=query, table_name=table_name, return_count=True, consistent_read=True):
            # time.sleep(3)
            logging.info(f"Written {row} to {table_name or client.config['table_name']}. "
                         f"It took {time.perf_counter() - st} seconds.")
            BENCHMARK['safe_put_to_ddb_calls'] += 1
            BENCHMARK['safe_put_to_ddb'] += time.perf_counter() - st
            return

    raise RuntimeError(f"Failed to write {row} to {table_name or client.config['table_name']} after "
                       f"trying for {time.perf_counter() - st} seconds")


def get_ddb_benchmark():
    """ Return benchmarking parameters for autotest table maintenance. """

    global BENCHMARK

    return BENCHMARK


def get_table_setup(hash_key: Tuple[str, str], range_key: Optional[Tuple[str, str]] = None,
                    table_name: str = None) -> Dict:
    """
    :param hash_key: tuple with hash key name and type
    :param range_key: tuple with range key name and type
    :return: Returns table structure compatible with boto3 dynamodb create_table(), with empty TableName
    """

    setup = {
        'AttributeDefinitions': [
            {
                'AttributeName': hash_key[0],
                'AttributeType': hash_key[1]
            }
        ],
        'TableName': table_name,
        'KeySchema': [
            {
                'AttributeName': hash_key[0],
                'KeyType': 'HASH'
            },
        ],
        'BillingMode': 'PAY_PER_REQUEST',
    }

    if range_key:
        setup['AttributeDefinitions'].append({'AttributeName': range_key[0], 'AttributeType': range_key[1]})
        setup['KeySchema'].append({'AttributeName': range_key[0], 'KeyType': 'RANGE'})

    return setup


def add_gsi(setup: Dict, index_name: str, hash_key: Tuple[str, str], range_key: Optional[Tuple[str, str]] = None,
            projection: str = 'ALL'):
    """
    :param setup: Table structure compatible with boto3 dynamodb create_table()
    :param index_name:
    :param hash_key: tuple with index hash key name and type
    :param range_key: tuple with index range key name and type
    :param projection: table projection compatible with boto3 dynamodb create_table ProjectionType parameter
    :return: Adds the indexes to the table setup. Returns table structure compatible with boto3 dynamodb create_table()
    """

    setup = deepcopy(setup)

    keys = [hash_key]
    if range_key:
        keys.append(range_key)

    for k_name, k_type in keys:
        attr_def = {'AttributeName': k_name, 'AttributeType': k_type}
        if attr_def not in setup['AttributeDefinitions']:
            setup['AttributeDefinitions'].append(attr_def)

    if 'GlobalSecondaryIndexes' not in setup:
        setup['GlobalSecondaryIndexes'] = []

    gsi_setup = {
        'IndexName': index_name,
        'KeySchema': [
            {'AttributeName': hash_key[0], 'KeyType': 'HASH'},
        ],
        'Projection': {
            'ProjectionType': projection
        }
    }

    if range_key:
        gsi_setup['KeySchema'].append({'AttributeName': range_key[0], 'KeyType': 'RANGE'})

    setup['GlobalSecondaryIndexes'].append(gsi_setup)

    return setup


class AutotestDdbManager:

    def __init__(self, tables: List[Dict] = None):
        if not tables:
            # By default, we create all main tables, but you can specify explicit ones
            tables = [autotest_dynamo_db_tasks_setup, autotest_dynamo_db_meta_setup,
                      autotest_dynamo_db_closed_tasks_setup, autotest_dynamo_db_retry_tasks_setup]
        self.tables = tables
        asyncio.run(self.create_ddbs(self.tables))


    async def create_ddbs(self, tables):
        X = await asyncio.gather(
            *[self.create_test_ddb(structure) for structure in tables]
        )


    async def clean_ddbs(self):
        X = await asyncio.gather(
            *[self.clean_ddb(structure) for structure in self.tables]
        )


    async def drop_ddbs(self):
        X = await asyncio.gather(
            *[self.drop_test_ddb(structure) for structure in self.tables]
        )


    async def clean_ddb(self, table_structure):
        """ Coroutine to clean a table. """
        name = table_structure['TableName']
        keys = [x['AttributeName'] for x in table_structure['KeySchema']]
        clean_dynamo_table(table_name=name, keys=tuple(keys))


    async def create_test_ddb(self, table_structure: Dict = None):
        """
        Asyncio coroutine.
        Creates a table with unique DB name using either sample generic schema or from argument.

        The function may also create Global Secondary Index (GSI) from table_structure.
        This significantly increases the creation time.

        ..  warning::

            There is a function with same name in the module which runs synchronously.
            We keep it for backwards compatibility.

        :param Dict table_structure: Should follow the boto3 create_table() request specification.
        """

        client = boto3.client('dynamodb')

        if not table_structure.get('TableName'):
            table_structure['TableName'] = get_autotest_ddb_name()

        name = table_structure['TableName']

        assert name.startswith('autotest_'), "Table for testing must start with an autotest prefix"

        logging.info("Creating table: %s", table_structure)
        # Try to send the request to create a table. If already exists - first drop it and then recreate.
        for retry in range(2):
            try:
                client.create_table(**table_structure)
                logging.info("Successfully sent request to create table %s", name)
                break

            except Exception as err:
                if "Table already exists" in str(err):
                    for retry2 in range(2):
                        try:
                            await self.drop_test_ddb(table_name=name)

                        except Exception as err2:
                            if "Table is being created" in str(err2):
                                await asyncio.sleep(retry2)

                            else:
                                raise Exception(err)

                else:
                    logging.exception(err, exc_info=True)
                    raise Exception(err)

        for i in range(34):  # ~10 minutes with exponential backoff
            response = client.describe_table(TableName=name)
            if response['Table']['TableStatus'] == 'ACTIVE':
                logging.info("Successfully created table %s after %s iterations.", name, i)
                break
            await asyncio.sleep(i)

        return name


    async def drop_test_ddb(self, table_structure: Dict = None):
        """
        Asyncio coroutine to drop temporary autotest DynamoDB table.
        ..  warning::

            There is a function with same name in the module which runs synchronously.
            We keep it for backwards compatibility.
        """
        name = table_structure['TableName'] or get_autotest_ddb_name()

        assert name.startswith('autotest_'), "Table for testing must start with an autotest prefix"

        client = boto3.client('dynamodb')

        for retry in range(3):
            try:
                client.delete_table(TableName=name)
                break

            except Exception as err2:
                if "Attempt to change a resource which is still in use" in str(err2):
                    time.sleep(retry)
                else:
                    raise Exception(err2)

        for i in range(34):  # ~10 minutes with exponential backoff
            try:
                response = client.describe_table(TableName=name)
                logging.info("Table %s is still %s", name, response['Table']['TableStatus'])
                await asyncio.sleep(i)
            except:
                logging.info("Successfully dropped table %s after %s iterations.", name, i)
                break


### Autotest DynamoDB structures ###

autotest_dynamo_db_setup = get_table_setup(hash_key=('hash_col', 'S'), range_key=('range_col', 'N'))
autotest_dynamo_db_with_index_setup = add_gsi(setup=autotest_dynamo_db_setup, index_name='autotest_index',
                                              hash_key=('hash_col', 'S'), range_key=('other_col', 'S'))

autotest_dynamo_db_tasks_draft = get_table_setup(hash_key=('task_id', 'S'),
                                                 table_name=get_autotest_ddb_name() + '_sosw_tasks')
autotest_dynamo_db_tasks_setup = add_gsi(setup=autotest_dynamo_db_tasks_draft, index_name='sosw_tasks_greenfield',
                                         hash_key=('labourer_id', 'S'), range_key=('greenfield', 'N'))

autotest_dynamo_db_meta_setup = get_table_setup(hash_key=('task_id', 'S'), range_key=('created_at', 'N'),
                                                table_name=get_autotest_ddb_name() + '_sosw_tasks_meta')

autotest_dynamo_db_closed_tasks_draft = get_table_setup(hash_key=('task_id', 'S'),
                                                        table_name=get_autotest_ddb_name() + '_sosw_closed_tasks')
autotest_dynamo_db_closed_tasks_setup = add_gsi(setup=autotest_dynamo_db_closed_tasks_draft,
                                                index_name='labourer_task_status_with_time',
                                                hash_key=('labourer_id_task_status', 'S'),
                                                range_key=('closed_at', 'N'))

autotest_dynamo_db_retry_tasks_draft = get_table_setup(hash_key=('labourer_id', 'S'), range_key=('task_id', 'S'),
                                                       table_name=get_autotest_ddb_name() + '_sosw_retry_tasks')
autotest_dynamo_db_retry_tasks_setup = add_gsi(setup=autotest_dynamo_db_retry_tasks_draft,
                                                index_name='labourer_id_greenfield',
                                                hash_key=('labourer_id', 'S'),
                                                range_key=('desired_launch_time', 'N'))
