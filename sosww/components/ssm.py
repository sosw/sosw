"""
AWS Systems Mananager helper functions.
Using these methods requires the Role to have permissions to access SSM for requested resources.
"""
import boto3
import csv
import json
import logging
import os
from collections import defaultdict

from .helpers import chunks

__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "1.4"
__license__ = "MIT"
__status__ = "Production"

__all__ = ['get_config', 'get_credentials_by_prefix']


def get_config(name):
    """
    Retrieve the Config from AWS SSM ParameterStore and return as a JSON parsed dictionary.

    .. :warning:
       In case the config with given `name` is missing or is not JSONable, the function DOES NOT raise.
       It returns empty dict(). This way you can call it safely, but if you really depend on this config from SSM,
       it is the responsibility of caller (Lambda itself) to check that something valueable is returned.

    :param str name:    Name of config to extract
    :rtype:             dict
    :return:            Config of some Controller
    """

    ssm = boto3.client('ssm')

    try:
        response = ssm.get_parameters(
                Names=[name],
                WithDecryption=True
        )
    except:
        response = ssm.get_parameters(
                Names=[name],
                WithDecryption=False
        )


    try:
        config = json.loads(response['Parameters'][0]['Value'])
    except (KeyError, IndexError, TypeError):
        config = dict()
    return config


def call_boto_with_pagination(f, **kwargs):
    """
    Invoke SSM functions with the ability to paginate results.

    :param str f:           SSM function to invoke.
    :param object kwargs:   Keyword arguments for the function to invoke.
    :rtype list
    :return:                List of paginated responses.
    """

    ssm_client = boto3.client('ssm')
    func = getattr(ssm_client, f)
    can_paginate = getattr(ssm_client, 'can_paginate')(f)

    if can_paginate:
        logging.debug(f"'SSM.{f}()' can natively paginate")
        paginator = ssm_client.get_paginator(f)
        response = paginator.paginate(**kwargs)
        return list(response)

    else:
        logging.debug(f"'SSM.{f}()' can not natively paginate")
        response_list = []
        response = func(**kwargs)
        response_list.append(response)
        while 'NextToken' in response:
            kwargs['NextToken'] = response['NextToken']
            response_list.append(func(**kwargs))
        return response_list


def get_credentials_by_prefix(prefix):
    """
    Retrieve the credentials with given `prefix` from AWS SSM ParameterStore and return as a dictionary.

    In ParameterStore the values `Name` must begin with `prefix_` and they must have Tag:Environment `(production|dev)`.
    The type of elements is expected to be SecureString. Regular strings could work, but not guaranteed.

    :param str prefix:  prefix of records to extract
    :rtype:             dict
    :return:            Some credentials
    """

    test = True if os.environ.get('STAGE') in ['test', 'autotest'] else False

    env_tag = 'production' if not test else 'dev'
    prefix = prefix if prefix.endswith('_') else prefix + '_'

    describe_params_response = call_boto_with_pagination('describe_parameters',
                                                         ParameterFilters=[
                                                             {"Key": "tag:Environment", "Values": [env_tag]},
                                                             {'Key': 'Name', 'Option': 'BeginsWith',
                                                              'Values': [prefix]}])

    logging.info(f"SSM.describe_parameters(prefix={prefix}) received response: {describe_params_response}")
    params = [param for obj in describe_params_response for param in obj['Parameters']]

    names = [param['Name'] for param in params]
    if not names:
        logging.warning(f"No credentials found in SSM ParameterStore with prefix {prefix} for Environment: {env_tag}")
        return dict()

    # This is supposed to work fine if you ask multiple keys even if some are not encrypted.
    # Anyway you should encrypt everything.
    decryption_required = any([True for param in params if param['Type'] == 'SecureString'])

    result = dict()
    for chunk_of_names in chunks(names, 10):
        get_params_response = call_boto_with_pagination('get_parameters', Names=chunk_of_names,
                                                        WithDecryption=decryption_required)
        logging.debug(f"SSM.get_parameters(names={chunk_of_names}) received response: {get_params_response}")

        # Update keys and values from this page of response to result. Removes the prefix away for keys.
        params = [param for obj in get_params_response for param in obj['Parameters']]
        if params:
            result.update(dict([(x['Name'].replace(prefix, ''), x['Value'] if x['Value'] != 'None' else None) for x in
                                params]))

    return result
