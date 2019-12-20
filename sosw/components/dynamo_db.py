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


__all__ = ['DynamoDbClient', 'clean_dynamo_table']
__author__ = "Nikolay Grishchenko, Sophie Fogel, Gil Halperin"
__version__ = "1.6"

import boto3
import datetime
import logging
import json
import os
import time
import pprint

from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

from .benchmark import benchmark
from .helpers import chunks, to_bool


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
            'hash_key': 'the_hash_key',
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

        # create a dynamodb client
        self.dynamo_client = boto3.client('dynamodb', region_name=config.get('region_name'))

        # storage for table description(s)
        self._table_descriptions: Optional[Dict[str, Dict]] = {}

        # initialize table store
        self._table_capacity = {}
        self.identify_dynamo_capacity(table_name=self.config['table_name'])

        self.stats = defaultdict(int)
        if not hasattr(self, 'row_mapper'):
            self.row_mapper = self.config.get('row_mapper')

        self.type_serializer = TypeSerializer()
        self.type_deserializer = TypeDeserializer()


    def identify_dynamo_capacity(self, table_name=None):
        """Identify and store the table capacity for a given table on the object

        Arguments:
            table_name {str} -- short name of the dynamo db table to analyze
        """
        # Use the config value if not provided
        if table_name is None:
            table_name = self.config['table_name']
            logging.debug("Got `table_name` from config: {table_name}")

        logging.debug(f"DynamoDB table name identified as {table_name}")

        # Fetch the actual configuration of the dynamodb table directly for
        table_description = self._describe_table(table_name)
        # Hash to the capacity
        table_capacity = table_description["Table"]["ProvisionedThroughput"]

        self._table_capacity[table_name] = {
            'read': int(table_capacity["ReadCapacityUnits"]),
            'write': int(table_capacity["WriteCapacityUnits"]),
        }


    def _describe_table(self, table_name: Optional[str] = None) -> Dict:
        """
        Returns description of the table from AWS. Response like:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.describe_table

        :return: Description of the table
        """

        table_name = self._get_validate_table_name(table_name)

        if self._table_descriptions and table_name in self._table_descriptions:
            return self._table_descriptions[table_name]
        else:
            table_description = self.dynamo_client.describe_table(TableName=table_name)
            self._table_descriptions[table_name] = table_description
            return table_description


    def get_table_keys(self, table_name: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """
        Returns table's hash key name and range key name

        :param table_name:
        :return: hash key and range key names
        """

        table_description = self._describe_table(table_name)
        key_schema: List[Dict[str, str]] = table_description['Table']['KeySchema']
        hash_key = range_key = None

        for key in key_schema:
            if key['KeyType'] == 'HASH':
                hash_key = key['AttributeName']
            elif key['KeyType'] == 'RANGE':
                range_key = key['AttributeName']

        return hash_key, range_key


    def get_table_indexes(self, table_name: Optional[str] = None) -> Dict:
        """
        Returns **active** indexes of the table: their hash key, range key, and projection type.

        .. code-block:: python

           {
               'index_1_name': {
                   'projection_type': 'ALL',  # One of: 'ALL'|'KEYS_ONLY'|'INCLUDE'
                   'hash_key': 'the_hash_key_column_name',
                   'range_key': 'the_range_key_column_name',  # Can be None if the index has no range key
                   'provisioned_throughput': {
                       'write_capacity': 5,
                       'read_capacity': 10
                   }
               },
               'index_2_name': ...
           }

        """

        indexes = {}

        table_description = self._describe_table(table_name)
        local_secondary_indexes = table_description['Table'].get('LocalSecondaryIndexes', [])
        global_secondary_indexes = table_description['Table'].get('GlobalSecondaryIndexes', [])

        for index in local_secondary_indexes + global_secondary_indexes:

            if index.get('IndexStatus') is not None and index.get('IndexStatus') != 'ACTIVE':
                # Only global sec. indexes has IndexStatus, and if it's not ready for use, we don't return it
                continue

            name = index['IndexName']
            projection_type = index['Projection']['ProjectionType']  # 'ALL'|'KEYS_ONLY'|'INCLUDE'

            key_schema = index['KeySchema']
            hash_key = range_key = None

            for key in key_schema:
                if key['KeyType'] == 'HASH':
                    hash_key = key['AttributeName']
                elif key['KeyType'] == 'RANGE':
                    range_key = key['AttributeName']

            # Get write & read capacity.
            # global sec. indexes have their own capacities, while a local sec. index uses the capacity of the table.
            write_capacity = index.get('ProvisionedThroughput', {}).get('WriteCapacityUnits') or \
                             table_description['ProvisionedThroughput']['WriteCapacityUnits']
            read_capacity = index.get('ProvisionedThroughput', {}).get('ReadCapacityUnits') or \
                            table_description['ProvisionedThroughput']['ReadCapacityUnits']

            indexes[name] = {
                'projection_type': projection_type,
                'hash_key': hash_key,
                'range_key': range_key,
                'provisioned_throughput': {
                    'write_capacity': write_capacity,
                    'read_capacity': read_capacity
                }
            }

        return indexes


    def dynamo_to_dict(self, dynamo_row: Dict, strict: bool = None, fetch_all_fields: Optional[bool] = None) -> Dict:
        """
        Convert the ugly DynamoDB syntax of the row, to regular dictionary.
        We currently support only String or Numeric values. Latest ones are converted to int or float.
        Takes settings from row_mapper.

        e.g.:               {'key1': {'N': '3'}, 'key2': {'S': 'value2'}}
        will convert to:    {'key1': 3, 'key2': 'value2'}

        :param dict dynamo_row:       DynamoDB row item
        :param bool strict:           DEPRECATED.
        :param bool fetch_all_fields: If False only row_mapper fields will be extracted from dynamo_row, else, all
                                      fields will be extracted from dynamo_row.
        :return: The row in a key-value format
        :rtype: dict
        """

        if strict is not None:
            logging.warning(f"dynamo_to_dict `strict` variable is deprecated in sosw 0.7.13+. "
                            f"Please replace it's usage with `fetch_all_fields` (and reverse the boolean value)")
        fetch_all_fields = fetch_all_fields if fetch_all_fields is not None else False if strict is None else not strict
        result = {}

        # Get fields from dynamo_row which are present in row mapper
        if not fetch_all_fields:
            for key, key_type in self.row_mapper.items():
                val_dict = dynamo_row.get(key)  # Ex: {'N': "1234"} or {'S': "myvalue"}
                if val_dict:
                    val = val_dict.get(key_type)  # Ex: 1234 or "myvalue"

                    # type_deserializer.deserialize() parses 'N' to `Decimal` type but it cant be parsed to a datetime
                    # so we cast it to either an integer or a float.
                    if key_type == 'N':
                        result[key] = float(val) if '.' in val else int(val)
                    elif key_type == 'M':
                        result[key] = self.dynamo_to_dict(val, fetch_all_fields=True)
                    elif key_type == 'S':
                        # Try to load to a dictionary if looks like JSON.
                        if val.startswith('{') and val.endswith('}') and \
                                not self.config.get('dont_json_loads_results'):
                            try:
                                result[key] = json.loads(val)
                            except ValueError:
                                logger.warning(f"A JSON-looking string failed to parse: {val}")
                                result[key] = val
                        else:
                            result[key] = val
                    else:
                        result[key] = self.type_deserializer.deserialize(val_dict)

        # Get all fields from dynamo_row
        else:
            for key, val_dict in dynamo_row.items():
                for val_type, val in val_dict.items():

                    # type_deserializer.deserialize() parses 'N' to `Decimal` type but it cant be parsed to a datetime
                    # so we cast it to either an integer or a float.
                    if val_type == 'N':
                        result[key] = float(val) if '.' in val else int(val)
                    elif val_type == 'M':
                        result[key] = self.dynamo_to_dict(val, fetch_all_fields=True)
                    elif val_type == 'S':
                        # Try to load to a dictionary if looks like JSON.
                        if val.startswith('{') and val.endswith('}') and \
                                not self.config.get('dont_json_loads_results'):
                            try:
                                result[key] = json.loads(val)
                            except ValueError:
                                logger.warning(f"A JSON-looking string failed to parse: {val}")
                                result[key] = val
                        else:
                            result[key] = val
                    else:
                        result[key] = self.type_deserializer.deserialize(val_dict)

        assert all(True for x in self.config['required_fields'] if result.get(x)), "Some `required_fields` are missing"
        return result


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

        result = {}

        # Keys from row mapper
        for key, key_type in self.row_mapper.items():
            val = row_dict.get(key)
            if val is not None:
                key_with_prefix = f"{add_prefix}{key}"
                if key_type == 'BOOL':
                    result[key_with_prefix] = {'BOOL': to_bool(val)}
                elif key_type == 'N':
                    result[key_with_prefix] = {'N': str(val)}
                elif key_type == 'S':
                    result[key_with_prefix] = {'S': str(val)}
                elif key_type == 'M':
                    result[key_with_prefix] = {'M': self.dict_to_dynamo(val, strict=False)}
                else:
                    result[key_with_prefix] = self.type_serializer.serialize(val)

        result_keys = result.keys()
        if add_prefix:
            result_keys = [x[len(add_prefix):] for x in result.keys()]

        # Keys which are not in row mapper
        for key in list(set(row_dict.keys()) - set(result_keys)):
            if not strict:
                val = row_dict.get(key)
                key_with_prefix = f"{add_prefix}{key}"
                if isinstance(val, bool):
                    result[key_with_prefix] = {'BOOL': to_bool(val)}
                elif isinstance(val, (int, float)) or (isinstance(val, str) and val.isnumeric()):
                    result[key_with_prefix] = {'N': str(val)}
                elif isinstance(val, str):
                    result[key_with_prefix] = {'S': str(val)}
                elif isinstance(val, dict):
                    result[key_with_prefix] = {'M': self.dict_to_dynamo(val, strict=False)}
                else:
                    result[key_with_prefix] = self.type_serializer.serialize(val)
            else:
                if key not in self.config.get('required_fields', []):
                    logger.warning(f"Field {key} is missing from row_mapper, so we can't convert it to DynamoDB "
                                   f"syntax. This is not a required field, so we continue, but please investigate "
                                   f"row: {row_dict}")
                else:
                    raise ValueError(f"Field {key} is missing from row_mapper, so we can't convert it to DynamoDB "
                                     f"syntax. This is a required field, so we can not continue. Row: {row_dict}")

        logger.debug(f"dict_to_dynamo result: {result}")
        return result


    def get_by_query(self, keys: Dict, table_name: Optional[str] = None, index_name: Optional[str] = None,
                     comparisons: Optional[Dict] = None, max_items: Optional[int] = None,
                     filter_expression: Optional[str] = None, strict: bool = None, return_count: bool = False,
                     desc: bool = False, fetch_all_fields: bool = None, expr_attrs_names: list = None) \
            -> Union[List[Dict], int]:
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
        :param bool strict: DEPRECATED.
        :param bool return_count: If True, will return the number of items in the result instead of the items themselves
        :param bool desc:    By default (False) the the values will be sorted ascending by the SortKey.
                             To reverse the order set the argument `desc = True`.
        :param bool fetch_all_fields: If False, will only get the attributes specified in the row mapper.
                                      If True, will get all attributes. Default is False.
        :param list expr_attrs_names: List of attributes names, in case if an attribute name begins with a number or
            contains a space, a special character, or a reserved word, you must use an expression attribute name to
            replace that attribute's name in the expression.
            Example, if the list ['session', 'key'] is received, then a new dict will be assigned to
            `ExpressionAttributeNames`:
            {'#session': 'session', '#key': 'key'}

        :return: List of items from the table, each item in key-value format
            OR the count if `return_count` is True
        """

        if strict is not None:
            logging.warning(f"get_by_query `strict` variable is deprecated in sosw 0.7.13+. "
                            f"Please replace it's usage with `fetch_all_fields` (and reverse the boolean value)")
        fetch_all_fields = fetch_all_fields if fetch_all_fields is not None else False if strict is None else not strict

        table_name = self._get_validate_table_name(table_name)

        filter_values = self.dict_to_dynamo(keys, add_prefix=':', strict=False)
        cond_expr_parts = []

        for key_attr_name in keys:
            # Check if key attribute name is in `expr_attrs_names`, and create a prefix
            # We add this prefix in case need to use `ExpressionAttributeNames`
            expr_attr_prefix = '#' if expr_attrs_names and key_attr_name in expr_attrs_names else ''

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
                cond_expr_parts.append(f"begins_with ({expr_attr_prefix}{key_attr_name}, :{key_attr_name})")

            elif compr == 'between':
                key = key_attr_name[11:]
                expr_attr_prefix = '#' if expr_attrs_names and key in expr_attrs_names else ''
                cond_expr_parts.append(f"{expr_attr_prefix}{key} between :st_between_{key} and :en_between_{key}")
            else:
                assert compr in ('=', '<', '<=', '>', '>='), f"Comparison not valid: {compr} for {key_attr_name}"
                cond_expr_parts.append(f"{expr_attr_prefix}{key_attr_name} {compr} :{key_attr_name}")

        cond_expr = " AND ".join(cond_expr_parts)

        select = ('ALL_ATTRIBUTES' if index_name is None else 'ALL_PROJECTED_ATTRIBUTES') if not return_count else 'COUNT'

        logger.debug(cond_expr, filter_values)
        query_args = {
            'TableName':                 table_name,
            'Select':                    select,
            'ExpressionAttributeValues': filter_values,  # Ex: {':key1_name': 'key1_value', ...}
            'KeyConditionExpression':    cond_expr  # Ex: "key1_name = :key1_name AND ..."
        }

        # In case of any of the attributes names are in the list of Reserved Words in DynamoDB or other situations when,
        # there is a need to specify ExpressionAttributeNames, then a dict should be passed to the query.
        if expr_attrs_names:
            query_args['ExpressionAttributeNames'] = {f'#{el}': el for el in expr_attrs_names}

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
            if return_count:
                raise Exception(f"DynamoDbCLient.get_by_query does not support `max_items` and `return_count` together")

        if desc:
            query_args['ScanIndexForward'] = False

        logger.debug(f"Querying dynamo: {query_args}")

        paginator = self.dynamo_client.get_paginator('query')
        response_iterator = paginator.paginate(**query_args)
        result = []

        if return_count:
            return sum([page['Count'] for page in response_iterator])

        for page in response_iterator:
            result += [self.dynamo_to_dict(x, fetch_all_fields=fetch_all_fields) for x in page['Items']]
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


    def get_by_scan(self, attrs=None, table_name=None, strict=None, fetch_all_fields=None):
        """
        Scans a table. Don't use this method if you want to select by keys. It is SLOW compared to get_by_query.
        Careful - don't make queries of too many items, this could run for a long time.

        Optional:

        :param dict attrs: Attribute names and values of the items we get. Can be empty to get the whole table.
        :param str table_name: Name of the dynamo table. If not specified, will use table_name from the config.
        :param bool strict: DEPRECATED.
        :param bool fetch_all_fields: If False, will only get the attributes specified in the row mapper.
            If True, will get all attributes. Default is False.
        :return: List of items from the table, each item in key-value format
        :rtype: list
        """

        if strict is not None:
            logging.warning(f"get_by_query `strict` variable is deprecated in sosw 0.7.13+. "
                            f"Please replace it's usage with `fetch_all_fields` (and reverse the boolean value)")
        fetch_all_fields = fetch_all_fields if fetch_all_fields is not None else False if strict is None else not strict

        response_iterator = self._build_scan_iterator(attrs, table_name)

        result = []
        for page in response_iterator:
            result += [self.dynamo_to_dict(x, fetch_all_fields=fetch_all_fields) for x in page['Items']]
            self.stats['dynamo_scan_queries'] += 1

        return result


    def get_by_scan_generator(self, attrs=None, table_name=None, strict=None, fetch_all_fields=None):
        """
        Scans a table. Don't use this method if you want to select by keys. It is SLOW compared to get_by_query.
        Careful - don't make queries of too many items, this could run for a long time.
        Same as get_by_scan, but yields parts of the results.

        Optional:

        :param dict attrs: Attribute names and values of the items we get. Can be empty to get the whole table.
        :param str table_name: Name of the dynamo table. If not specified, will use table_name from the config.
        :param bool strict: DEPRECATED.
        :param bool fetch_all_fields: If False, will only get the attributes specified in the row mapper.
            If false, will get all attributes. Default is True.
        :return: List of items from the table, each item in key-value format
        :rtype: list
        """

        if strict is not None:
            logging.warning(f"get_by_query `strict` variable is deprecated in sosw 0.7.13+. "
                            f"Please replace it's usage with `fetch_all_fields` (and reverse the boolean value)")
        fetch_all_fields = fetch_all_fields if fetch_all_fields is not None else False if strict is None else not strict

        response_iterator = self._build_scan_iterator(attrs, table_name)
        for page in response_iterator:
            self.stats['dynamo_scan_queries'] += 1
            yield [self.dynamo_to_dict(x, fetch_all_fields=fetch_all_fields) for x in page['Items']]


    def _build_scan_iterator(self, attrs=None, table_name=None):
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


    def batch_get_items_one_table(self, keys_list, table_name=None, max_retries=0, retry_wait_base_time=0.2,
                                  strict=None, fetch_all_fields=None):
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
        :param bool strict: DEPRECATED.
        :param bool fetch_all_fields: If False, will only get the attributes specified in the row mapper.
                                      If True, will get all attributes. Default is False.
        :return: List of items from the table
        :rtype: list
        """

        if strict is not None:
            logging.warning(f"batch_get_items_one_table `strict` variable is deprecated in sosw 0.7.13+. "
                            f"Please replace it's usage with `fetch_all_fields` (and reverse the boolean value)")
        fetch_all_fields = fetch_all_fields if fetch_all_fields is not None else False if strict is None else not strict

        table_name = self._get_validate_table_name(table_name)

        # Convert given keys to dynamo syntax
        query_keys = [self.dict_to_dynamo(item) for item in keys_list]

        # Check if we skipped something - if we did, try again.
        def get_unprocessed_keys(db_result):
            return 'UnprocessedKeys' in db_result and db_result['UnprocessedKeys'] \
                   and table_name in db_result['UnprocessedKeys'] and db_result['UnprocessedKeys'][table_name]['Keys']

        all_items = []

        for query_keys_chunk in chunks(query_keys, 100):

            batch_get_item_query = {
                'RequestItems': {
                    table_name: {
                        'Keys': query_keys_chunk
                    }
                }
            }

            logger.debug(f"batch_get_item query: {batch_get_item_query}")
            latest_result = self.dynamo_client.batch_get_item(**batch_get_item_query)
            logger.debug(f"latest_result: {latest_result}")
            unprocessed_keys = get_unprocessed_keys(latest_result)
            all_items += latest_result['Responses'][table_name]
            logger.debug(f"batch_get_items_one_table response: {latest_result}")

            if unprocessed_keys:
                # Retry several times
                retry_num = 0
                wait_time = retry_wait_base_time
                while unprocessed_keys and retry_num < max_retries:
                    logger.warning(f"batch_get_item action did NOT finish successfully.")
                    time.sleep(wait_time)
                    batch_get_item_query['RequestItems'][table_name]['Keys'] = unprocessed_keys
                    latest_result = self.dynamo_client.batch_get_item(**batch_get_item_query)
                    logger.debug(f"latest_result: {latest_result}")
                    all_items += latest_result['Responses'][table_name]
                    retry_num += 1
                    wait_time *= 2
                    unprocessed_keys = get_unprocessed_keys(latest_result)

            # After the retries still we have a bad result... then raise Exception
            if get_unprocessed_keys(latest_result):
                raise Exception(f"batch_get_items action failed for table {table_name}, keys_list {keys_list}")

        result = []
        for item in all_items:
            result.append(self.dynamo_to_dict(item, fetch_all_fields=fetch_all_fields))

        return result


    def build_put_query(self, row, table_name=None, overwrite_existing=True):
        table_name = self._get_validate_table_name(table_name)
        dynamo_formatted_row = self.dict_to_dynamo(row, strict=False)
        query = {
            'TableName': table_name,
            'Item':      dynamo_formatted_row
        }
        if not overwrite_existing:
            hash_key = self.config['hash_key']
            query['ConditionExpression'] = f"attribute_not_exists({hash_key})"
        return query


    def build_delete_query(self, delete_keys: Dict, table_name: str = None):
        table_name = self._get_validate_table_name(table_name)
        dynamo_formatted_row = self.dict_to_dynamo(delete_keys, strict=False)
        query = {
            'TableName': table_name,
            'Key':       dynamo_formatted_row
        }
        return query


    def put(self, row, table_name=None, overwrite_existing=True):
        """
        Adds a row to the database

        :param dict row:                The row to add to the table. key is column name, value is value.
        :param string table_name:       Name of the dynamo table to add the row to.
        :param bool overwrite_existing: Overwrite the existing row if True, otherwise will raise an exception if exists.
        """

        table_name = self._get_validate_table_name(table_name)

        put_query = self.build_put_query(row, table_name, overwrite_existing)
        logger.debug(f"Put to DB: {put_query}")

        dynamo_response = self.dynamo_client.put_item(**put_query)

        logger.debug(f"Response from dynamo {dynamo_response}")

        self.stats['dynamo_put_queries'] += 1


    def create(self, row, table_name=None):
        self.put(row, table_name, overwrite_existing=False)


    # @benchmark
    def update(self, keys: Dict, attributes_to_update: Optional[Dict] = None,
               attributes_to_increment: Optional[Dict] = None, table_name: Optional[str] = None,
               condition_expression: Optional[str] = None, attributes_to_remove: Optional[List[str]] = None):
        """
        Updates an item in DynamoDB. Will create a new item if doesn't exist.
        IMPORTANT - If you want to make sure it exists, use ``patch`` method

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
        :param list attributes_to_remove: Will remove these attributes from the record
        :param str condition_expression: Condition Expression that must be fulfilled on the object to update.
        :param str table_name: Name of the table
        """

        table_name = self._get_validate_table_name(table_name)

        if not attributes_to_update and not attributes_to_increment and not attributes_to_remove:
            raise ValueError(f"In dynamodb.update, please specify either attributes_to_update "
                             f"or attributes_to_increment or attributes_to_remove")

        expression_attributes = {}
        update_set_val_expr_parts = []
        attribute_values = {}
        if attributes_to_update:
            for col in attributes_to_update:
                update_set_val_expr_parts.append(f"#{col} = :{col}")
                expression_attributes[f"#{col}"] = col

        if attributes_to_increment:
            for col in attributes_to_increment:
                update_set_val_expr_parts.append(f"#{col} = if_not_exists(#{col}, :zero) + :{col}")
                expression_attributes[f"#{col}"] = col
                attribute_values.update({'zero': '0'})

        keys = self.dict_to_dynamo(keys, strict=False)

        attribute_values.update((attributes_to_update or {}))
        attribute_values.update(attributes_to_increment or {})
        attribute_values = self.dict_to_dynamo(attribute_values.copy(), add_prefix=":", strict=False)

        update_expr_parts = []

        if update_set_val_expr_parts:
            set_expression = "SET " + ", ".join(update_set_val_expr_parts)
            update_expr_parts.append(set_expression)

        if attributes_to_remove:
            remove_expression = "REMOVE " + ", ".join(attributes_to_remove)
            update_expr_parts.append(remove_expression)

        update_item_query = {
            'Key':                       keys,  # Ex. {'key_name':   'key_value', ...}
            'TableName':                 table_name,
            'UpdateExpression':          " ".join(update_expr_parts)  # Ex. "SET #attr_name = :attr_name ..."
        }

        if expression_attributes:
            update_item_query['ExpressionAttributeNames'] = expression_attributes  # Ex. {'#attr_name': 'attr_name', ..}
        if attribute_values:
            update_item_query['ExpressionAttributeValues'] = attribute_values  # Ex. {':attr_name': 'some_value', ...}

        if condition_expression:
            expr, values = self._parse_filter_expression(condition_expression)
            update_item_query['ConditionExpression'] = expr
            if values:
                update_item_query['ExpressionAttributeValues'] = update_item_query.get('ExpressionAttributeValues', {})
                update_item_query['ExpressionAttributeValues'].update(values)

        logger.debug(f"Updating an item, query: {update_item_query}")
        response = self.dynamo_client.update_item(**update_item_query)
        logger.debug(f"Update result: {response}")
        self.stats['dynamo_update_queries'] += 1


    def patch(self, keys: Dict, attributes_to_update: Optional[Dict] = None,
              attributes_to_increment: Optional[Dict] = None, table_name: Optional[str] = None,
              attributes_to_remove: Optional[List[str]] = None):
        """
        Updates an item in DynamoDB. Will fail if an item with these keys does not exist.
        """

        hash_key = self.config['hash_key']
        condition_expression = f'attribute_exists {hash_key}'
        self.update(keys=keys, attributes_to_update=attributes_to_update,
                    attributes_to_increment=attributes_to_increment, table_name=table_name,
                    condition_expression=condition_expression,
                    attributes_to_remove=attributes_to_remove)


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

    def get_capacity(self, table_name=None):
        """Fetches capacity for data tables

        Keyword Arguments:
            table_name {str} -- DynamoDB (default: {None})

        Returns:
            dict -- read/write capacity for the table requested
        """

        if table_name is None:
            logging.debug(self.config)
            table_name = self.config['table_name']

        logging.debug(f"DynamoDB table name identified as {table_name}")

        if table_name in self._table_capacity.keys():
            return self._table_capacity[table_name]
        else:
            self.identify_dynamo_capacity(table_name=table_name)
            return self._table_capacity[table_name]


    def sleep_db(self, last_action_time: datetime.datetime, action: str):
        """
        Sleeps between calls to dynamodb (if it needs to).
        Uses the table's capacity to decide how long it needs to sleep.

        :param last_action_time: Last time when we did this action (read/write) to this dynamo table
        :param action: "read" or "write"
        """

        capacity = self.get_capacity()[action]  # Capacity per second
        time_between_actions = 1 / capacity

        time_elapsed = datetime.datetime.now().timestamp() - last_action_time.timestamp()

        time_to_sleep = time_between_actions - time_elapsed

        if time_to_sleep > 0:
            logging.debug(f"Sleeping {time_to_sleep} sec")
            time.sleep(time_to_sleep)


    def reset_stats(self):
        """
        Cleans statistics.
        """
        self.stats = defaultdict(int)


def clean_dynamo_table(table_name='autotest_dynamo_db', keys=('hash_col', 'range_col'), filter_expression=None):
    """
    Cleans the DynamoDB Table. Only for autotest tables.

    :param str table_name: name of the table
    :param tuple keys: the keys of the table
    :param str filter_expression:  Supports regular comparisons and `between`. Input must be a regular human string
        e.g. 'key <= 42', 'name = marta', 'foo between 10 and 20', etc.

    .. warning:: There are some reserved words that woud not work with
                 Filter Expression in case they are attribute names. Fix this one day.

    """

    assert table_name.startswith('autotest_')

    client = boto3.client('dynamodb')
    paginator = client.get_paginator('scan')
    stats = defaultdict(int)

    query_args = {
        'TableName': table_name,
        'Select':    'ALL_ATTRIBUTES',
    }

    if filter_expression:
        # We use DynamoDbClient class only for static method to construct Filter Expression. No need for config.
        dynamo_client = DynamoDbClient(config={'row_mapper': {'name': 'S', }})

        query_args['FilterExpression'] = {}
        expr, values = dynamo_client._parse_filter_expression(filter_expression)
        query_args['FilterExpression'] = expr
        query_args['ExpressionAttributeValues'] = values

    response_iterator = paginator.paginate(**query_args)

    for page in response_iterator:
        stats['dynamo_scan_queries'] += 1

        for row in page['Items']:
            client.delete_item(
                    TableName=table_name,
                    Key={key: row[key] for key in keys}
            )

            stats['deleted'] += 1
        logger.debug(f"clean_dynamo_table() of '{table_name}': {stats}")

    logger.info(f"clean_dynamo_table() of '{table_name}': {stats}")
