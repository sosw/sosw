"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - Serverless Orchestrator of Serverless Workers

    The MIT License (MIT)
    Copyright (C) 2024  sosw core contributors <info@sosw.app>

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

__all__ = ['Processor', 'LambdaGlobals', 'get_lambda_handler']
__author__ = "Nikolay Grishchenko, Gil Halperin"

try:
    from aws_lambda_powertools import Logger

    logger = Logger()

except ImportError:
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

import boto3
import os

from collections import defaultdict
from importlib import import_module
from typing import Dict
from sosw.components.benchmark import benchmark
from sosw.components.config import get_config
from sosw.components.helpers import *
from sosw.components.dynamo_db import DynamoDbClient


class Processor:
    """
    Core Processor class template. All the main components (Worker, Orchestrator and Scheduler) inherit from this one.
    You can also use this class as parent for some of your standalone Lambdas, but we strictly encourage you to use
    `Worker` class in case you are running functions under `sosw` orchestration.


    ``get_ddbc(prefix: str) -> DynamoDbClient:``

    Lazily initializes and retrieves a DynamoDB client configured for a specific table and schema validation.

    This method initializes custom DynamoDB clients based on the provided prefix. If a client with the
    specified prefix has already been initialized, it returns the existing client. If not, it looks in the
    Processor config for a prefixed dynamodb config (e.g. for prefix ``project_a`` -> ``project_a_dynamo_db_config``).
    The dynamo_db client will be initialized as ``self.project_a_dynamo_db_client``.

    This is particularly useful for scenarios requiring schema validation, transformation of DynamoDB
    syntax to dictionary format, and other operations beyond the capabilities of the raw boto3 client.

    :param str prefix:  The prefix for the DynamoDB client configuration and naming.
    :raises ValueError: If the provided prefix is not supported by the available configuration.

    """

    DEFAULT_CONFIG = {}

    aws_account: str = None
    aws_region: str = os.getenv('AWS_REGION', None)
    ddb_names: list = None
    stats: dict = None
    result: dict = None


    def __init__(self, custom_config=None, **kwargs):
        """
        Initialize the Processor.
        Recursively Updates the default config with parameters from DynamoDB / SSM, then from provided custom config.
        """

        self.test = kwargs.get('test') or True if os.environ.get('STAGE') in ['test', 'autotest'] else False

        if global_vars.lambda_context:
            if invoked_function_arn := getattr(global_vars.lambda_context, 'invoked_function_arn', None):
                self.aws_account = trim_arn_to_account(invoked_function_arn)
                logger.info("Setting self.aws_account from Lambda context to: %s", self.aws_account)

        self.init_config(custom_config=custom_config)
        logger.info("Final %s processor config", self.__class__.__name__)
        logger.info(self.config)

        self.stats = defaultdict(int)
        self.result = defaultdict(int)

        self.register_clients(self.config.get('init_clients', []))


    def init_config(self, custom_config: Dict = None):
        """
        By default, tries to initialize config from ``DEFAULT_CONFIG`` or as an empty dictionary.
        After that, a specific custom config of the Lambda will recursively update the existing one.
        The last step is update config recursively with a passed custom_config.

        Overwrite this method if custom logic of recursive updates in configs is required.

        ..  note:: Read more about :ref:`Config_Sourse`

        :param Dict custom_config: dict with custom configurations
        """

        # Initialize config from default config
        self.config = self.DEFAULT_CONFIG or {}
        # Update config recursively from any existing lambda function config
        self.config = recursive_update(self.config,
                                       self.get_config(f"{os.environ.get('AWS_LAMBDA_FUNCTION_NAME')}_config") or {})
        # Update config recursively from custom config
        self.config = recursive_update(self.config, custom_config or {})


    @benchmark
    def register_clients(self, clients):
        """
        Initialize the given `clients` and assign them to self with suffix `_client`.

        Clients are imported from `components` or `managers`. Name of the module must be underscored name of Client.
        Name of the Class must be name of `client` with either of the suffixes ('Manager' or 'Client').

        .. warning::
           To be implemented!

           If you follow these rules and put the module in package `components` of your Lambda,
           you can just provide the `clients` in custom_config when initializing the Processor.

        TODO This method supports a too many ways of class initialization for backwards compatibility
        that it becomes a mess soon. Need to describe best practices and start deprecation in future versions.

        :param list clients:    List of names of clients.
        """

        client_suffixes = ['Manager', 'Client']

        import_paths = [
            lambda x: f"components.{x}",
            lambda x: f"managers.{x}",
            lambda x: f"sosw.components.{x}",
            lambda x: f"sosw.managers.{x}",
        ]

        # # Initialize required clients
        for service in clients:
            module_name = camel_case_to_underscore(service)

            for path in import_paths:
                try:
                    some_module = import_module(path(module_name))
                    logger.debug(f"Imported {service} from {path(module_name)}")
                    break
                except Exception:
                    pass

            else:
                # The other supported option is to load boto3 client if it exists.
                try:
                    setattr(self, f"{module_name}_client", boto3.client(module_name))
                    continue
                except Exception:
                    raise RuntimeError(f"Failed to import for service {module_name}. Component naming problem.")

            for suffix in client_suffixes:
                try:
                    some_class = getattr(some_module, f"{service}{suffix}")
                except AttributeError as e:
                    logger.debug(f"Didn't find {service} with suffix {suffix} in module {module_name}")
                    continue

                some_client_config = self.config.get(f"{module_name}_config")
                logger.debug(f"Found config for {module_name}: {some_client_config}")

                # Send configs one of the two ways as `config` or `custom_config` for some backwards compatibility
                if some_client_config:
                    if suffix == 'Manager':
                        setattr(self, f"{module_name}_client", some_class(custom_config=some_client_config))
                    elif suffix == 'Client':
                        setattr(self, f"{module_name}_client", some_class(config=some_client_config))

                else:
                    setattr(self, f"{module_name}_client", some_class())
                logger.info(f"Successfully registered {module_name}_client")
                break
            else:
                raise RuntimeError(f"Failed to import {service} from {some_module}. "
                                   f"Tried suffixes for class: {client_suffixes}")


    def __call__(self, event, reset_result: bool = True):
        """
        Call the Processor.
        You can either call super() at the end of your child function or completely overwrite this function.

        :param reset_result: Whether to reset the result after the processor call. Defaults to True.
        """

        # Update the stats for number of calls.
        self.stats['processor_calls'] += 1
        if reset_result:
            self.result = defaultdict(int)


    def __pre_call__(self, recursive: bool = True):
        """
        Reset the result of the processor.
        Cleans statistics other than specified for the lifetime of processor.
        Makes sense for Processors initialized outside the scope of `lambda_handler`.
        Call this before actually calling the processor.

        Be careful about circular get_stats() calls from child classes.
        If required overwrite get_stats() with recursive = False.
        :param recursive:   Merge stats from self.***_client.
        """
        self.result = defaultdict(int)
        self.reset_stats(recursive)


    @staticmethod
    def get_config(name):
        """
        Returns config by name from DynamoDB config or SSM. Override this to provide your config handling method.

        :param name: Name of the config
        :rtype: dict
        """

        return get_config(name)


    @property
    def _account(self):
        """
        Get current AWS Account to construct different ARNs.

        We don't have this parameter in Environmental variables, only can parse from Context. It is stored
        in ``global_vars`` and is supposed to be passed by your `lambda_handler` during initialization.

        As a fallback for cases when we use ``Processor`` not in the Lambda environment, we have a lazy autodetection
        mechanism using STS, but it is pretty heavy (~0.3 seconds).

        Some things to note:
         - We store this value in class variable for fast access
         - It uses Lazy initialization.
         - We first try from context and only if not provided - use the autodetection.
        """

        if not self.aws_account:
            self.aws_account = boto3.client('sts').get_caller_identity().get('Account')

        return self.aws_account


    @property
    def _region(self):
        """
        Property fetched from AWS Lambda Environmental variables.
        """
        return self.aws_region


    def _c(self, path: str):
        """
        Shortcut to access values from the Processor config.

        E.g. ``val = self._c('path.to.param')``

        Is similar to: ``val = self.config.get('path', {}).get('to', {}).get('param', None)``
        """
        return recursive_matches_extract(self.config, path)


    @benchmark
    def get_ddbc(self, prefix: str) -> DynamoDbClient:
        """
        Lazily initializes and retrieves a DynamoDB client configured for a specific table and schema validation.

        This method initializes custom DynamoDB clients based on the provided prefix. If a client with the
        specified prefix has already been initialized, it returns the existing client. If not, it looks in the
        Processor config for a prefixed dynamodb config (e.g. for prefix ``project_a`` -> ``project_a_dynamo_db_config``).
        The dynamo_db client will be initialized as ``self.project_a_dynamo_db_client``.

        This is particularly useful for scenarios requiring schema validation, transformation of DynamoDB
        syntax to dictionary format, and other operations beyond the capabilities of the raw boto3 client.

        :param str prefix:  The prefix for the DynamoDB client configuration and naming.
        :raises ValueError: If the provided prefix is not supported by the available configuration.
        """
        if not self.ddb_names:
            self.ddb_names = list([x.split('_dynamo_db_config')[0] for x in
                                   filter(lambda x: x.endswith('_dynamo_db_config'), self.config)])

        if prefix not in self.ddb_names:
            raise ValueError(f"get_ddbc() method supports only prefixes: {self.ddb_names}")

        name = f"{prefix}_dynamo_db_client"
        if not hasattr(self, name):
            setattr(self, name, DynamoDbClient(self.config[f'{prefix}_dynamo_db_config']))

        return getattr(self, name)


    def get_stats(self, recursive: bool = True):
        """
        Return statistics of operations performed by current instance of the Class.

        Statistics of custom clients existing in the Processor is also aggregated by default.
        Clients must be initialized as `self.some_client` ending with `_client` suffix (e.g. self.dynamo_db_client).
        Clients must also have their own get_stats() methods implemented.

        Be careful about circular get_stats() calls from child classes.
        If required overwrite get_stats() with recursive = False.

        .. code-block::python

           def get_stats(self, recursive=False):
               return super().get_stats(recursive=False)

        :param recursive:   Merge stats from self.***_client.
        :rtype:     dict
        :return:    Statistics counter of current Processor instance.
        """

        if recursive:
            for some_client in [x for x in dir(self) if x.endswith('_client')]:
                try:
                    self.stats.update(getattr(self, some_client).get_stats())
                    logger.info(f"Updated Processor stats with stats of {some_client}")
                except Exception:
                    logger.debug(f"{some_client} doesn't have get_stats() implemented. Recommended to fix this.")

        return dict(self.stats)


    def reset_stats(self, recursive: bool = True):
        """
        Cleans statistics other than specified for the lifetime of processor.
        All the parameters with prefix *'total_'* are also preserved.

        The function makes sense if your Processor lives outside the scope of `lambda_handler`.

        Be careful about circular get_stats() calls from child classes.
        If required overwrite reset_stats() with recursive = False.

        .. code-block::python

           def reset_stats(self, recursive=False):
               return super().reset_stats(recursive=False)

        :param recursive:   Reset stats from self.***_client.
        """

        # Temporary save values that are supposed to survive reset_stats().
        preserved = defaultdict(int)
        preserved.update({k: v for k, v in self.stats.items()
                          if k in self.config.get('lifetime_stats_params', []) or k.startswith('total_')})

        # Update them with current values
        for k, v in self.stats.items():
            if not isinstance(v, (int, float)):
                continue
            if not (k in self.config.get('lifetime_stats_params', []) or k.startswith('total_')):
                preserved[f'total_{k}'] += v

        # Recreate a new version of stats to avoid mess in the memory between dictionaries.
        self.stats = defaultdict(int)
        self.stats.update(preserved)

        if recursive:
            for some_client in [x for x in dir(self) if x.endswith('_client')]:
                try:
                    getattr(self, some_client).reset_stats()
                except Exception:
                    pass


    def die(self, message="Unknown Failure"):
        """
        Logs current Processor stats and `message`. Then raises RuntimeError with `message`.

        If there is access to publish SNS messages, the method will also try to publish to the topic configured as
        `dead_sns_topic` or `'SoswWorkerErrors'`.

        :param str message: Description of failure.
        """

        logger.exception(message)

        result = {'status': 'failed'}
        result.update(self.get_stats())
        logger.info(result)

        try:
            sns_recipient = self.config.get('dead_sns_topic', 'SoswWorkerErrors')
            sns_topic_arn = f'arn:aws:sns:{self._region}:{self._account}:{sns_recipient}'
            sns_subject = f"{os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'Some Function')} died"

            sns = boto3.client('sns')
            sns.publish(TopicArn=sns_topic_arn, Subject=sns_subject, Message=message)
        except Exception:
            logger.exception("Failed to send SNS message to Alarms.")

        raise SystemExit(1)


    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Destructor.

        Close SQLAlchemy session if exists.
        Flask-SQLAlchemy does this for you, but the `self.sql` can also be a pointer to the global in the container
        of some functions (scope outside of lambda_handler). In this case we try to reset the session manually.

        Other database connections are also closed if found.
        """

        try:
            self.sql.sqldb.session.remove()
        except Exception:
            pass

        try:
            self.conn.close()
        except Exception:
            pass


# Global lambda processor placeholder
_processor = None

# Global lambda context placeholder
_lambda_context = None


class LambdaGlobals:
    """
    Global placeholder for global_vars that we want to preserve in the lifetime of the Lambda Container.
    e.g. once initiailised the given Processor, we keep it alive in the container to minimize warm-run time.

    This namespace also contains the lambda_context which should be reset by `get_lambda_handler` method.
    See Worker examples in documentation for more info.
    """


    def __init__(self):
        """
        Reset the lambda context for every reinitialization.
        The Processor may stay alive in the scope of Lambda container, but the context is unique per invocation.
        The Lambda Globals should also be reset by `get_lambda_handler` method.
        """
        global _lambda_context
        _lambda_context = None


    @property
    def lambda_context(self):
        global _lambda_context
        return _lambda_context


    @lambda_context.setter
    def lambda_context(self, val):
        global _lambda_context
        _lambda_context = val


    @property
    def processor(self):
        global _processor
        return _processor


    @processor.setter
    def processor(self, val):
        global _processor
        _processor = val


def get_lambda_handler(processor_class, global_vars=None, custom_config=None):
    """
    Return a reference to the entry point of the lambda function.

    :param processor_class:  Callable processor class.
    :param global_vars:      Lambda's global variables (processor, context).
    :param custom_config:    Custom configuration to pass the processor constructor.
    :return: Function reference for the lambda handler.
    """

    if global_vars is None:
        logger.error("Your Lambda did not pass global_vars. It should be an instance of LambdaGlobals class, "
                     "initialised in your Lambda function at the root level. Some functionality will break soon.")
        global_vars = LambdaGlobals()


    def lambda_handler(event, context):
        """
        Entry point for the lambda function.

        :param dict event:      Lambda function event.
        :param object context:  Lambda function context.
        :return: Result of the lambda function call.
        """

        if event.get('logging_level'):
            logger.setLevel(event.get('logging_level'))

        logger.info("Called %s lambda of version %s with __name__: %s, context: %s",
                    os.environ.get('AWS_LAMBDA_FUNCTION_NAME'), os.environ.get('AWS_LAMBDA_FUNCTION_VERSION'),
                    __name__, context)
        logger.info(event)

        test = event.get('test') or True if os.environ.get('STAGE') in ['test', 'autotest'] else False

        global_vars.lambda_context = context

        if global_vars.processor is None:
            global_vars.processor = processor_class(custom_config=custom_config, test=test)

        result = global_vars.processor(event)

        logger.info(global_vars.processor.get_stats())

        global_vars.processor.reset_stats()
        global_vars.processor.reset_stats(recursive=True)

        logger.info(result)

        return result


    return lambda_handler


# Global placeholder for global_vars.
global_vars = LambdaGlobals()
