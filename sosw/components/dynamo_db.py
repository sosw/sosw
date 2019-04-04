__all__ = ['DynamoDbClient', 'clean_dynamo_table']
__author__ = "Nikolay Grishchenko, Sophie Fogel"
__version__ = "1.6"

import boto3
import logging
import json
import os
import time
import pprint

from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union

from .benchmark import benchmark
from .helpers import chunks


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DynamoDbClient:
    """
    Has default methods for different types of DynamoDB tables.

    The current implementation supports only one fixed table during initialization,
    but you are free to initialize multiple simultaneous dynamo_clients in your Lambda with different configs.

    Config should have a mapping for the field types and required fields.
    Config example:

    .. code-block:: python

        {
            'row_mapper':     {
                'col_name_1':      'N', # Number
                'col_name_2':      'S', # String
            },
            'required_fields': ['col_name_1']
            'table_name': 'some_table_name',  # If a table is not specified, this table will be used.
            'dont_json_loads_results': True  # Use this if you don't want to convert json strings into json
        }

    """


    def __init__(self, config):
        assert isinstance(config, dict), "Config must be provided during DynamoDbClient initialization"

        # If this is a test, make sure the table is a test table
        if os.environ.get('STAGE') == 'test' and 'table_name' in config:
            assert config['table_name'].startswith('autotest_') or config['table_name'] == 'config', \
                f"Bad table name {config['table_name']} in autotest"

        self.config = config

        if not str(config.get('table_name')).startswith('autotest_mock_'):
            self.dynamo_client = boto3.client('dynamodb')
        else:
            logger.info(f"Initialized DynamoClient without boto3 client for table {config.get('table_name')}")

        self.stats = defaultdict(int)
        if not hasattr(self, 'row_mapper'):
            self.row_mapper = self.config.get('row_mapper')


    @benchmark
    def dynamo_to_dict(self, dynamo_row, strict=True):
        """
        Convert the ugly DynamoDB syntax of the row, to regular dictionary.
        We currently support only String or Numeric values. Latest ones are converted to int or float.
        Takes settings from row_mapper.

        e.g.:               {'key1': {'N': '3'}, 'key2': {'S': 'value2'}}
        will convert to:    {'key1': 3, 'key2': 'value2'}

        :param dict dynamo_row:     DynamoDB row item
        :param boolean  strict:     If True only row_mapper fields will be extracted from dynamo_row, else, all
                                    fields will be extracted from dynamo_row.
        :rtype: dict
        :return:                    Human readable row from dynamo
        """

        result = {}
        if strict:
            for key, key_type in self.row_mapper.items():
                val_dict = dynamo_row.get(key)  # Ex: {'N': "1234"} or {'S': "myvalue"}
                if val_dict:
                    val = val_dict.get(key_type)  # Ex: 1234 or "myvalue"
                    if key_type == 'N':
                        result[key] = float(val) if '.' in val else int(val)
                    elif key_type == 'S':
                        # Try to load to a dictionary if looks like JSON.
                        if val.startswith('{') and val.endswith('}') and not self.config.get('dont_json_loads_results'):
                            try:
                                result[key] = json.loads(val)
                            except ValueError:
                                logger.warning("A JSON-looking string failed to parse: {}".format(val))
                                result[key] = val
                        else:
                            result[key] = val
                    else:
                        raise RuntimeError(f"DynamoDbClient.dynamo_to_dict() found that self.row_mapper has "
                                           f"unsupported key_type: {key_type}. DynamoDbClient now supports only "
                                           f"'S' or 'N' types. Others must be JSON-ified.")
        else:
            for key, key_type_and_val in dynamo_row.items():  # {'key1': {'Type1': 'val2'}, 'key2': {'Type2': 'val2'}}
                for key_type, val in key_type_and_val.items():  # Ex: {'N': "1234"} or {'S': "myvalue"}
                    if key_type == 'N':
                        result[key] = float(val) if '.' in val else int(val)
                    elif key_type == 'S':
                        # Try to load to a dictionary if looks like JSON.
                        if val.startswith('{') and val.endswith('}') and not self.config.get('dont_json_loads_results'):
                            try:
                                result[key] = json.loads(val)
                            except ValueError:
                                logger.warning(f"A JSON-looking string failed to parse: {val}")
                                result[key] = val
                        else:
                            result[key] = val
                    else:
                        raise RuntimeError(f"DynamoDbClient.dynamo_to_dict() found that self.row_mapper has "
                                           f"unsupported key_type: {key_type}. DynamoDbClient now supports only "
                                           f"'S' or 'N' types. Others must be JSON-ified.")

        assert all(True for x in self.config['required_fields'] if result.get(x)), "Some `required_fields` are missing"
        return result


    @benchmark
    def dict_to_dynamo(self, row_dict, add_prefix=None, strict=True):
        """
        Convert the row from regular dictionary to the ugly DynamoDB syntax. Takes settings from row_mapper.

        e.g.                {'key1': 'value1', 'key2': 'value2'}
        will convert to:    {'key1': {'Type1': 'value1'}, 'key2': {'Type2': 'value2'}}

        :param dict row_dict:   A row we want to convert to dynamo syntax.
        :param str add_prefix:  A string prefix to add to the key in the result dict. Useful for queries like update.
        :param bool strict:     If False, will get the type from the value in the dict (this works for numbers and
                                strings). If True, won't add them if they're not in the required_fields, and if they
                                are, will raise an error.

        :return:                DynamoDB Task item
        :rtype:                 dict
        """

        if add_prefix is None:
            add_prefix = ''

        result = {f"{add_prefix}{key}": {key_type: str(row_dict.get(key))} for (key, key_type) in
                  self.row_mapper.items()
                  if row_dict.get(key) is not None}
        result_keys = result.keys()
        if add_prefix:
            result_keys = [x[len(add_prefix):] for x in result.keys()]
        for key in list(set(row_dict.keys()) - set(result_keys)):
            if not strict:
                val = row_dict.get(key)
                key_with_prefix = f"{add_prefix}{key}"
                if isinstance(val, (int, float)) or (isinstance(val, str) and val.isnumeric()):
                    result[key_with_prefix] = {'N': str(row_dict.get(key))}
                else:
                    result[key_with_prefix] = {'S': str(row_dict.get(key))}
            else:
                if not key in self.config.get('required_fields', []):
                    logger.warning(f"Field {key} is missing from row_mapper, so we can't convert it to DynamoDB "
                                   f"syntax. This is not a required field, so we continue, but please investigate "
                                   f"row: {row_dict}")
                else:
                    raise ValueError(f"Field {key} is missing from row_mapper, so we can't convert it to DynamoDB "
                                     f"syntax. This is a required field, so we can not continue. Row: {row_dict}")
        logger.debug(result)
        return result


    @benchmark
    def get_by_query(self, keys: Dict, table_name: Optional[str] = None, index_name: Optional[str] = None,
                     comparisons: Optional[Dict] = None, max_items: Optional[int] = None,
                     filter_expression: Optional[str] = None, strict: bool = True, return_count: bool = False,
                     desc: bool = False) -> Union[List[Dict], int]:
        """
        Get an item from a table, by some keys. Can specify an index.
        If an index is not specified, will query the table.
        IMPORTANT: You must specify the rows you expect to be converted in row mapper in config, otherwise you won't
        get them in the result.
        If you want to get items from dynamo by non-key attributes, this method is not for you.

        :param dict keys: Keys and values of the items we get.
            You must specify the hash key, and can optionally also add the range key.
            Example, in a table where the hash key is 'hk' and the range key is 'rk':
            * {'hk': 'cat', 'rk': '123'}
            * {'hk': 'cat'}

        Optional

        :param str table_name:  Name of the dynamo table. If not specified, will use table_name from the config.
        :param str index_name:  Name of the secondary index in the table. If not specified, will query the table itself.
        :param dict comparisons: Type of comparison for each key. If a key is not mentioned, comparison type will be =.
            Valid values: `=`, `<`, `<=`, `>`, `>=`, `begins_with`.
            Comparisons only work for the range key.
            Example: if keys={'hk': 'cat', 'rk': 100} and comparisons={'rk': '<='} -> will get items where rk <= 100

        :param int max_items:   Limit the number of items to fetch.
        :param str filter_expression:  Supports regular comparisons and `between`. Input must be a regular human string
            e.g. 'key <= 42', 'name = marta', 'foo between 10 and 20', etc.
        :param bool strict:     If True, will only get the attributes specified in the row mapper.
                                If false, will get all attributes. Default is True.
        :param bool return_count: If True, will return the number of items in the result instead of the items themselves
        :param bool desc:    By default (False) the the values will be sorted ascending by the SortKey.
                             To reverse the order set the argument `desc = True`.

        :return: List of items from the table, each item in key-value format
            OR the count if `return_count` is True
        """

        table_name = self._get_validate_table_name(table_name)

        filter_values = self.dict_to_dynamo(keys, add_prefix=':', strict=False)
        cond_expr_parts = []

        for key_attr_name in keys:
            # Find comparison for key. The formatting of conditions could be different, so a little spaghetti.
            if key_attr_name.startswith('st_between_'):  # This is just a marker to construct a custom expression later
                compr = 'between'
            elif key_attr_name.startswith('en_between_'):  # This attribute is used in the expression with st_between
                continue
            elif comparisons:
                compr = comparisons.get(key_attr_name) or '='
            else:
                compr = '='

            if compr == 'begins_with':
                cond_expr_parts.append(f"begins_with ({key_attr_name}, :{key_attr_name})")

            elif compr == 'between':
                key = key_attr_name[11:]
                cond_expr_parts.append(f"{key} between :st_between_{key} and :en_between_{key}")
            else:
                assert compr in ('=', '<', '<=', '>', '>='), f"Comparison not valid: {compr} for {key_attr_name}"
                cond_expr_parts.append(f"{key_attr_name} {compr} :{key_attr_name}")

        cond_expr = " AND ".join(cond_expr_parts)

        select = ('ALL_ATTRIBUTES' if index_name is None else 'ALL_PROJECTED_ATTRIBUTES') if not return_count else 'COUNT'

        logger.debug(cond_expr, filter_values)
        query_args = {
            'TableName':                 table_name,
            'Select':                    select,
            'ExpressionAttributeValues': filter_values,  # Ex: {':key1_name': 'key1_value', ...}
            'KeyConditionExpression':    cond_expr  # Ex: "key1_name = :key1_name AND ..."
        }

        # In case we have a filter expression, we parse it and add variables (values) to the ExpressionAttributeValues
        # Expression is also transformed to use these variables.
        if filter_expression:
            expr, values = self._parse_filter_expression(filter_expression)
            query_args['FilterExpression'] = expr
            query_args['ExpressionAttributeValues'].update(values)

        if index_name:
            query_args['IndexName'] = index_name

        if max_items:
            query_args['PaginationConfig'] = {'MaxItems': max_items}

        if desc:
            query_args['ScanIndexForward'] = False

        logger.debug(f"Querying dynamo: {query_args}")

        paginator = self.dynamo_client.get_paginator('query')
        response_iterator = paginator.paginate(**query_args)
        result = []
        for page in response_iterator:
            if return_count:
                return page['Count']

            result += [self.dynamo_to_dict(x, strict=strict) for x in page['Items']]
            self.stats['dynamo_get_queries'] += 1
            if max_items and len(result) >= max_items:
                break

        return result[:max_items] if max_items else result


    def _parse_filter_expression(self, expression: str) -> Tuple[str, Dict]:
        """
        Converts FilterExpression to Dynamo syntax. We still do not support some operators. Feel free to implement:
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

        Supported: regular comparators, between, attribute_[not_]exists

        :return:  Returns a tuple of the transformed expression and extracted variables already Dynamo formatted.
        """

        assert isinstance(expression, str), f"Filter expression must be a string: {expression}"

        words = [x.strip() for x in expression.split()]

        # Filter Expression should be 2, 3 or 5 words. See doc for more details.
        # This must be a function
        if len(words) == 2:
            operator, key = words
            assert operator.lower() in ('attribute_exists', 'attribute_not_exists')
            result_expr, result_values = f"{operator} ({key})", {}

        # This must be a regular comparison
        elif len(words) == 3:
            key, operator, value = words
            assert operator in ('=', '<>', '<', '<=', '>', '>='), f"Unsupported operator for filtering: {expression}"

            # It is important to add prefix to value here to avoid attribute naming conflicts for example
            # in conditional_update expressions. e.g you update some field only if it's value is matching condition.
            result_expr = f"{key} {operator} :filter_{key}"
            result_values = self.dict_to_dynamo({f"filter_{key}": words[-1]}, add_prefix=':', strict=False)

        # This must be `between` statement.
        elif len(words) == 5:
            assert (words[1].lower(), words[3].lower()) == ('between', 'and'), \
                f"Unsupported expression for Filtering: {expression}"
            key = words[0]
            result_expr = f"{key} between :st_between_{key} and :en_between_{key}"
            result_values = self.dict_to_dynamo({f"st_between_{key}": words[2],
                                                 f"en_between_{key}": words[4]}, add_prefix=':', strict=False)
        else:
            raise ValueError(f"Unsupported expression for Filtering: {expression}")

        return result_expr, result_values


    @benchmark
    def get_by_scan(self, attrs=None, table_name=None, strict=True):
        """
        Scans a table. Don't use this method if you want to select by keys. It is SLOW compared to get_by_query.
        Careful - don't make queries of too many items, this could run for a long time.

        Optional:

        :param dict attrs: Attribute names and values of the items we get. Can be empty to get the whole table.
        :param str table_name: Name of the dynamo table. If not specified, will use table_name from the config.
        :param bool strict: If True, will only get the attributes specified in the row mapper.
            If false, will get all attributes. Default is True.
        :return: List of items from the table, each item in key-value format
        :rtype: list
        """

        response_iterator = self._build_scan_iterator(attrs, table_name, strict)

        result = []
        for page in response_iterator:
            result += [self.dynamo_to_dict(x, strict=strict) for x in page['Items']]
            self.stats['dynamo_scan_queries'] += 1

        return result


    @benchmark
    def get_by_scan_generator(self, attrs=None, table_name=None, strict=True):
        """
        Scans a table. Don't use this method if you want to select by keys. It is SLOW compared to get_by_query.
        Careful - don't make queries of too many items, this could run for a long time.
        Same as get_by_scan, but yields parts of the results.

        Optional:

        :param dict attrs: Attribute names and values of the items we get. Can be empty to get the whole table.
        :param str table_name: Name of the dynamo table. If not specified, will use table_name from the config.
        :param bool strict: If True, will only get the attributes specified in the row mapper.
            If false, will get all attributes. Default is True.
        :return: List of items from the table, each item in key-value format
        :rtype: list
        """

        response_iterator = self._build_scan_iterator(attrs, table_name, strict)
        for page in response_iterator:
            self.stats['dynamo_scan_queries'] += 1
            yield [self.dynamo_to_dict(x, strict=strict) for x in page['Items']]


    def _build_scan_iterator(self, attrs=None, table_name=None, strict=True):
        table_name = self._get_validate_table_name(table_name)

        filter_values = None
        cond_expr = None
        if attrs:
            filter_values = self.dict_to_dynamo(attrs, add_prefix=':', strict=False)

            cond_expr_parts = []

            for key_attr_name in attrs:
                cond_expr_parts.append(f"{key_attr_name} = :{key_attr_name}")

            cond_expr = " AND ".join(cond_expr_parts)

        query_args = {
            'TableName': table_name,
            'Select':    'ALL_ATTRIBUTES',
        }
        if cond_expr:
            query_args['FilterExpression'] = cond_expr
        if filter_values:
            query_args['ExpressionAttributeValues'] = filter_values

        logger.debug(f"Scanning dynamo: {query_args}")

        paginator = self.dynamo_client.get_paginator('scan')
        response_iterator = paginator.paginate(**query_args)
        return response_iterator


    def batch_get_items_one_table(self, keys_list, table_name=None, max_retries=0, retry_wait_base_time=0.2):
        """
        Gets a batch of items from a single dynamo table.
        Only accepts keys, can't query by other columns.

        :param list keys_list: A list of the keys of the items we want to get. Gets the items that match the given keys.
                               If some key doesn't exist - it just skips it and gets the others.
                               e.g. [{'hash_col': '1, 'range_col': 2}, {'hash_col': 3}]
                               - will get a row where `hash_col` is 1 and `range_col` is 2, and also all rows where
                               `hash_col` is 3.

        Optional

        :param str table_name:
        :param int max_retries: If failed to get some items, retry this many times. Waiting between retries is
                                multiplied by 2 after each retry, so `retries` shouldn't be a big number.
                                Default is 1.
        :param int retry_wait_base_time: Wait this much time after first retry. Will wait twice longer in each retry.
        :return: List of items from the table
        :rtype: list
        """

        table_name = self._get_validate_table_name(table_name)

        # Convert given keys to dynamo syntax
        query_keys = [self.dict_to_dynamo(item) for item in keys_list]

        batch_get_item_query = {
            'RequestItems': {
                table_name: {
                    'Keys': query_keys
                }
            }
        }

        logger.debug(f"batch_get_item query: {batch_get_item_query}")

        db_result = self.dynamo_client.batch_get_item(**batch_get_item_query)
        logger.debug(f"batch_get_items_one_table response: {db_result}")


        # Check if we skipped something - if we did, try again.
        def is_action_incomplete(db_result):
            return 'UnprocessedKeys' in db_result and db_result['UnprocessedKeys'] \
                   and table_name in db_result['UnprocessedKeys'] and db_result['UnprocessedKeys'][table_name]


        if is_action_incomplete(db_result):
            # Retry several times
            retry_num = 0
            wait_time = retry_wait_base_time
            while is_action_incomplete(db_result) and retry_num < max_retries:
                logger.warning(f"batch_get_item action did NOT finish successfully.")
                time.sleep(wait_time)
                db_result = self.dynamo_client.batch_get_item(**batch_get_item_query)
                retry_num += 1
                wait_time *= 2

        # After the retries still we have a bad result... then raise Exception
        if is_action_incomplete(db_result):
            raise Exception(f"batch_get_items action failed for table {table_name}, keys_list {keys_list}")

        items = db_result['Responses'][table_name]

        result = []
        for item in items:
            result.append(self.dynamo_to_dict(item))

        return result


    def build_put_query(self, row, table_name=None):
        table_name = self._get_validate_table_name(table_name)
        dynamo_formatted_row = self.dict_to_dynamo(row, strict=False)
        query = {
            'TableName': table_name,
            'Item':      dynamo_formatted_row
        }
        return query


    def build_delete_query(self, delete_keys: Dict, table_name: str = None):
        table_name = self._get_validate_table_name(table_name)
        dynamo_formatted_row = self.dict_to_dynamo(delete_keys, strict=False)
        query = {
            'TableName': table_name,
            'Key':       dynamo_formatted_row
        }
        return query


    @benchmark
    def put(self, row, table_name=None):
        """
        Adds a row to the database
        :param dict row:            The row to add to the table. key is column name, value is value.
        :param string table_name:   Name of the dynamo table to add the row to
        """

        table_name = self._get_validate_table_name(table_name)

        put_query = self.build_put_query(row, table_name)
        logger.debug(f"Put to DB: {put_query}")

        dynamo_response = self.dynamo_client.put_item(**put_query)

        logger.debug(f"Response from dynamo {dynamo_response}")

        self.stats['dynamo_put_queries'] += 1


    @benchmark
    def update(self, keys: Dict, attributes_to_update: Optional[Dict] = None,
               attributes_to_increment: Optional[Dict] = None, table_name: Optional[str] = None,
               condition_expression: Optional[str] = None):
        """
        Updates an item in DynamoDB.

        :param dict keys:
            Keys and values of the row we update.
            Example, in a table where the hash key is 'hk' and the range key is 'rk':
            {'hk': 'cat', 'rk': '123'}
        :param dict attributes_to_update:
            Dict of the attributes to be updated.
            Can contain both existing attributes and new attributes.
            Will update existing, and create new attributes.
            Example: {'col_name': 'some_value'}
        :param dict attributes_to_increment:
            Attribute names to increment, and the value to increment by. If the attribute doesn't exist, will create it.
            Example: {'some_counter': '3'}
        :param str condition_expression: Condition Expression that must be fulfilled on the object to update.
        :param str table_name: Name of the table
        """

        table_name = self._get_validate_table_name(table_name)

        if not attributes_to_update and not attributes_to_increment:
            raise ValueError(f"In dynamodb.update, please specify either attributes_to_update "
                             f"or attributes_to_increment")

        expression_attributes = {}
        update_expr_parts = []
        attribute_values = {}
        if attributes_to_update:
            for col in attributes_to_update:
                update_expr_parts.append(f"#{col} = :{col}")
                expression_attributes[f"#{col}"] = col

        if attributes_to_increment:
            for col in attributes_to_increment:
                update_expr_parts.append(f"#{col} = if_not_exists(#{col}, :zero) + :{col}")
                expression_attributes[f"#{col}"] = col
                attribute_values.update({'zero': '0'})

        keys = self.dict_to_dynamo(keys, strict=False)

        attribute_values.update((attributes_to_update or {}))
        attribute_values.update(attributes_to_increment or {})
        attribute_values = self.dict_to_dynamo(attribute_values.copy(), add_prefix=":", strict=False)

        update_expr = "SET " + ", ".join(update_expr_parts)

        update_item_query = {
            'ExpressionAttributeNames':  expression_attributes,  # Ex. {'#attr_name': 'attr_name', ...}
            'ExpressionAttributeValues': attribute_values,  # Ex. {':attr_name': 'some_value', ...}
            'Key':                       keys,  # Ex. {'key_name':   'key_value', ...}
            'TableName':                 table_name,
            'UpdateExpression':          update_expr  # Ex. "SET #attr_name = :attr_name AND ..."
        }

        if condition_expression:
            expr, values = self._parse_filter_expression(condition_expression)
            update_item_query['ConditionExpression'] = expr
            update_item_query['ExpressionAttributeValues'].update(values)

        logger.debug(f"Updating an item, query: {update_item_query}")
        response = self.dynamo_client.update_item(**update_item_query)
        logger.debug(f"Update result: {response}")
        self.stats['dynamo_update_queries'] += 1


    def delete(self, keys: Dict, table_name: Optional[str] = None):
        """

        :param dict keys: Keys and values of the row we delete.
        :param table_name:
        """

        query = self.build_delete_query(keys, table_name)
        self.dynamo_client.delete_item(**query)


    def make_put_transaction_item(self, row, table_name=None):
        return {'Put': self.build_put_query(row, table_name)}


    def make_delete_transaction_item(self, row, table_name):
        return {'Delete': self.build_delete_query(row, table_name)}


    def transact_write(self, *transactions: Dict):
        """
        Executes many write transaction. Can execute operations on different tables.
        Will split transactions to chunks - because transact_write_items accepts up to 10 actions.
        WARNING: If you're expecting a transaction on more than 10 operations - AWS DynamoDB doesn't support it.

        .. code-block:: python

            dynamo_db_client = DynamoDbClient(config)
            t1 = dynamo_db_client.make_put_transaction_item(row, table_name='table1')
            t2 = dynamo_db_client.make_delete_transaction_item(row, table_name='table2')
            dynamo_db_client.transact_write(t1, t2)

        """

        supported_actions = ['Put', 'Delete']
        for t in transactions:
            assert isinstance(t, dict), "transaction must be a dictionary"
            assert len(t) == 1, "one transaction must contain only one operation"
            action = list(t.keys())[0]
            assert action in supported_actions, f"Bad action '{action}'. " \
                                                f"Supported actions: {', '.join(supported_actions)}"
            assert isinstance(t[action], dict), f"transaction[{action}] must be a dictionary. bad type: " \
                                                f"{type(t[action])}"

        for t_chunk in chunks(transactions, 10):
            logger.debug(f"Transactions: \n{pprint.pformat(t_chunk)}")

            response = self.dynamo_client.transact_write_items(TransactItems=t_chunk)

            self.stats['dynamo_transact_write_operations'] += 1
            logger.debug(f"Response from transact_write_items: {response}")


    def _get_validate_table_name(self, table_name=None):
        if table_name is None:
            table_name = self.config.get('table_name')

            if table_name is None:
                raise RuntimeError("Failed to dynamo action. no 'table_name' in config  and table_name wasn't "
                                   "specified in the arguments.")
        if os.environ.get('STAGE') == 'test':
            assert table_name.startswith('autotest_') or table_name == 'config', f"Bad table name in test: {table_name}"

        return table_name


    def get_stats(self):
        """
        Return statistics of operations performed by current instance of the Class.

        :return:    -   dict    - key: int statistics.
        """
        return self.stats


    def reset_stats(self):
        """
        Cleans statistics.
        """
        self.stats = defaultdict(int)


def clean_dynamo_table(table_name='autotest_dynamo_db', keys=('hash_col', 'range_col')):
    """
    Cleans the DynamoDB Table. Only for autotest tables.

    :param str table_name: name of the table
    :param tuple keys: the keys of the table
    :return:
    """

    assert table_name.startswith('autotest_')

    client = boto3.client('dynamodb')
    for row in client.scan(TableName=table_name)['Items']:
        client.delete_item(
                TableName=table_name,
                Key={key: row[key] for key in keys}
        )
