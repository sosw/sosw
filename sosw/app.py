"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - a framework for bootstrapping AWS Lambda functions

    The MIT License (MIT)
    Copyright (C) 2026  sosw core contributors <info@sosw.app>

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

__all__ = ['Processor', 'LambdaGlobals', 'get_lambda_handler', 'LOG_REDACTED_VALUE', 'LOG_SENSITIVE_KEY_PARTS',
           'LOG_COUNTER_KEY_WORDS']
__author__ = "Nikolay Grishchenko, Gil Halperin"

try:
    from aws_lambda_powertools import Logger

    logger = Logger()

except ImportError:
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

import json
import os
import re

import boto3

from collections import defaultdict
from decimal import Decimal
from importlib import import_module
from typing import Dict, Any
from urllib.parse import parse_qsl, urlencode

from sosw.components.benchmark import benchmark
from sosw.components.config import get_config
from sosw.components.helpers import *
from sosw.components.dynamo_db import DynamoDbClient


#: Value substituted in logs for anything a sensitive key holds. See ``_redact_for_logging``.
LOG_REDACTED_VALUE = '***REDACTED***'

#: A key of a dict is considered sensitive (and its value is redacted in logs) when the lowercase
#: form of the key contains any of these substrings. Consumers may extend the matching by reassigning
#: ``sosw.app.LOG_SENSITIVE_KEY_PARTS`` (read from the module globals at call time).
LOG_SENSITIVE_KEY_PARTS = (
    'authorization', 'cookie', 'token', 'secret', 'password', 'passwd', 'api-key', 'api_key', 'apikey',
    'x-origin-verify', 'credential', 'signature', 'private-key', 'private_key',
)

#: A sensitive key that has one of these words among the words of its own name keeps a numeric
#: value: counters like ``max_tokens``, ``inputTokens`` or ``tokenCount`` are useful in logs and
#: carry no secret. Words are the lowercase chunks of the key split on any non-alphanumeric
#: character and on camelCase boundaries, so ``account_password`` does not match ``count``. Reassign
#: ``sosw.app.LOG_COUNTER_KEY_WORDS`` to extend the matching (read at call time, like the others).
LOG_COUNTER_KEY_WORDS = ('tokens', 'count')

#: Matches a form-encoded body of ``name=value`` pairs joined with ``&``. See ``_redact_body``.
_FORM_BODY_PATTERN = re.compile(r'^[^=&\s]+=[^&\s]*(&[^=&\s]+=[^&\s]*)*$')


def _key_words(key):
    """
    Split a dict key into lowercase words — the matching unit of :py:data:`LOG_COUNTER_KEY_WORDS`.

    The key is split on any non-alphanumeric character and on camelCase boundaries, so
    ``max_tokens``, ``inputTokens`` and ``tokenCount`` all yield plain lowercase words.

    :param key:  Dict key to split.
    :return:     Lowercase words of the key.
    :rtype:      list
    """
    return [word.lower() for word in re.findall(r'[A-Z]?[a-z0-9]+|[A-Z]+(?![a-z])', str(key))]


def _redact_query_string(query):
    """
    Return a copy of a raw URL query string with the values of sensitive parameters masked.

    Parameter names are matched against :py:data:`LOG_SENSITIVE_KEY_PARTS` exactly like dict keys;
    parameters with non-sensitive names keep their values, and blank values are preserved.

    :param str query:   Raw query string of the request.
    :return:            The query string with sensitive parameter values replaced by
                        :py:data:`LOG_REDACTED_VALUE`.
    :rtype:             str
    """
    pairs = []
    for name, value in parse_qsl(query, keep_blank_values=True):
        sensitive = any(part in name.lower() for part in LOG_SENSITIVE_KEY_PARTS)
        pairs.append((name, LOG_REDACTED_VALUE if sensitive else value))
    return urlencode(pairs, safe='*')


def _content_type_starts_with(container, prefix):
    """
    Check whether a sibling ``headers`` dict carries a ``content-type`` header starting with a prefix.

    The ``headers`` key of the container and the header name itself match in any case, and the
    prefix compares against the lowercase header value. See ``_body_is_form_encoded`` and
    ``_redact_body``.

    :param dict container:  Dict holding the ``body`` key.
    :param str prefix:      Lowercase content-type prefix, e.g. ``multipart/``.
    :return:                True when a sibling headers dict has such a content-type header.
    :rtype:                 bool
    """
    for key, headers in container.items():
        if str(key).lower() != 'headers' or not isinstance(headers, dict):
            continue
        for name, value in headers.items():
            if str(name).lower() == 'content-type' and isinstance(value, str) and value.lower().startswith(prefix):
                return True

    return False


def _body_is_form_encoded(body, container):
    """
    Check whether a string body should be treated as ``application/x-www-form-urlencoded``.

    True when a sibling ``headers`` dict (any key case) carries a ``content-type`` header (any key
    case) starting with ``application/x-www-form-urlencoded``, or when the body itself matches the
    ``name=value&name=value`` shape of ``_FORM_BODY_PATTERN``.

    :param str body:        Raw body of the request or response.
    :param dict container:  Dict holding the ``body`` key.
    :return:                True when the body should be redacted as a form.
    :rtype:                 bool
    """
    return (_content_type_starts_with(container, 'application/x-www-form-urlencoded')
            or _FORM_BODY_PATTERN.match(body) is not None)


def _redact_body(body, container):
    """
    Return a log-safe copy of a string request or response body.

    A body of a dict with a truthy ``isBase64Encoded`` sibling is replaced with
    :py:data:`LOG_REDACTED_VALUE` — base64 is reversible, so it is never logged. A body whose
    sibling ``headers`` declare a ``multipart/`` content type is replaced with the marker as well.
    A body whose stripped form starts with ``{`` or ``[`` is parsed as JSON, redacted recursively
    and re-serialized (``ensure_ascii=False``); a body too deeply nested to redact, or one that
    looks like JSON but fails to parse and is not form-encoded, is replaced with the marker as
    well — it may be a truncated payload still carrying secrets. Any other string that
    ``_body_is_form_encoded`` accepts is redacted like a query string; everything else is returned
    unchanged.

    :param str body:        Raw body of the request or response.
    :param dict container:  Dict holding the ``body`` key (e.g. the whole event or response).
    :return:                The redacted body, or the original string when there is nothing to redact.
    :rtype:                 str
    """
    if container.get('isBase64Encoded'):
        return LOG_REDACTED_VALUE

    if _content_type_starts_with(container, 'multipart/'):
        return LOG_REDACTED_VALUE

    if body.strip()[:1] in ('{', '['):
        try:
            return json.dumps(_redact_for_logging(json.loads(body)), ensure_ascii=False)
        except ValueError:
            # Not JSON after all: only a form-shaped body is still worth redacting as a form.
            if not _body_is_form_encoded(body, container):
                return LOG_REDACTED_VALUE
        except RecursionError:
            # Too deeply nested to redact - and it may still hold secrets.
            return LOG_REDACTED_VALUE

    if _body_is_form_encoded(body, container):
        return _redact_query_string(body)

    return body


def _redact_for_logging(data):
    """
    Return a redacted copy of ``data`` suitable for logging. Never mutates the input.

    A key of a dict is sensitive when its lowercase form contains any substring from
    :py:data:`LOG_SENSITIVE_KEY_PARTS`. The value of a sensitive key is replaced with
    :py:data:`LOG_REDACTED_VALUE`, except for ``None`` and booleans, and for numbers (``int``,
    ``float``, ``Decimal``) under keys that have a whole word of :py:data:`LOG_COUNTER_KEY_WORDS`
    (the words of a key are its lowercase chunks split on any non-alphanumeric character and on
    camelCase boundaries, so ``max_tokens`` and ``tokenCount`` keep their counters while a numeric
    ``password`` does not). Two non-sensitive keys get special handling: a string ``rawQueryString``
    has the values of its sensitive parameters masked, and a string ``body`` is redacted by
    ``_redact_body`` (JSON parsed and re-serialized, form-encoded values masked, base64-encoded,
    multipart and JSON-looking-but-unparsable bodies replaced with the marker). Dicts, lists and
    tuples are copied and processed recursively;
    anything else is returned as is. All constants are looked up in the module globals at call
    time, so reassigning them changes the behaviour.

    :param data:    Dict, list, tuple or scalar value to prepare for logging.
    :return:        Redacted copy of ``data`` with the values of sensitive keys masked.
    :rtype:         dict | list | tuple | object
    """
    if isinstance(data, dict):
        redacted = {}
        for key, value in data.items():
            key_lower = str(key).lower()
            if any(part in key_lower for part in LOG_SENSITIVE_KEY_PARTS):
                # None and booleans are kept as is, numbers only under counter-like keys; everything
                # else a sensitive key holds (str, list, dict, bytes, a plain number) is masked.
                if value is None or isinstance(value, bool) or (
                        isinstance(value, (int, float, Decimal))
                        and any(word in _key_words(key) for word in LOG_COUNTER_KEY_WORDS)):
                    redacted[key] = value
                else:
                    redacted[key] = LOG_REDACTED_VALUE
            elif key_lower == 'rawquerystring' and isinstance(value, str):
                redacted[key] = _redact_query_string(value)
            elif key_lower == 'body' and isinstance(value, str):
                redacted[key] = _redact_body(value, data)
            else:
                redacted[key] = _redact_for_logging(value)
        return redacted

    if isinstance(data, list):
        return [_redact_for_logging(item) for item in data]

    if isinstance(data, tuple):
        return tuple(_redact_for_logging(item) for item in data)

    return data


def _log_redacted(value):
    """
    Return the redacted copy of ``value`` for a handler log line, or the redaction marker.

    A guard around ``_redact_for_logging``: logging must never fail an invocation, so when the
    redacted copy cannot be built — a value too deeply nested to copy, a query string that cannot
    be re-encoded — a warning naming only the exception type is logged and
    :py:data:`LOG_REDACTED_VALUE` is returned instead.

    :param value:   Event or result object to prepare for logging.
    :return:        Redacted copy of ``value``, or the marker when it cannot be built.
    :rtype:         object
    """
    try:
        return _redact_for_logging(value)
    except Exception as exc:
        logger.warning("Could not redact the value for logging (%s), logging the redaction marker instead",
                       type(exc).__name__)
        return LOG_REDACTED_VALUE


def _derive_test_flag(explicit_flag=None):
    """
    Resolve the effective ``test`` flag of the Processor or the lambda handler.

    An explicitly provided flag (the ``test`` keyword of the Processor constructor, or the ``test`` key
    of the Lambda event) always wins. When no flag is provided (``None``), the flag is derived from the
    ``STAGE`` environment variable: the ``test`` and ``autotest`` stages run in test mode.

    :param explicit_flag:   Explicitly provided value of the flag or None when not provided.
    :return:                The effective test flag.
    """

    if explicit_flag is not None:
        return explicit_flag

    return os.environ.get('STAGE') in ['test', 'autotest']


class Processor:
    """
    Core Processor class template. This is the base class of the framework: every Lambda built on
    ``sosw`` subclasses it (directly, or through specializations like
    :py:class:`~sosw.lambda_api.LambdaApi`). It provides layered configuration, automatic client
    registration, statistics counters and a uniform entry point.


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

    # Set to True in a subclass to skip the lookup of the per-function config in DynamoDB / SSM.
    # See `init_config()` for details.
    DISABLE_DDB_CONFIG = False

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

        self.test = _derive_test_flag(kwargs.get('test'))

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

        The lookup of the specific custom config of the Lambda (``{AWS_LAMBDA_FUNCTION_NAME}_config``
        from DynamoDB / SSM) can be skipped either by setting the class attribute
        ``DISABLE_DDB_CONFIG = True`` in your Processor, or by passing a truthy ``disable_ddb_config``
        key in the ``DEFAULT_CONFIG`` or ``custom_config``. ``DEFAULT_CONFIG`` and ``custom_config``
        are still applied in this case.

        Overwrite this method if custom logic of recursive updates in configs is required.

        ..  note:: Read more about :ref:`Config_Sourse`

        :param Dict custom_config: dict with custom configurations
        """

        custom_config = custom_config or {}

        # Initialize config from default config
        self.config = self.DEFAULT_CONFIG or {}

        # Update config recursively from any existing lambda function config, unless explicitly disabled.
        disable_ddb_config = (self.DISABLE_DDB_CONFIG or self.config.get('disable_ddb_config')
                              or custom_config.get('disable_ddb_config'))
        if not disable_ddb_config:
            self.config = recursive_update(
                    self.config, self.get_config(f"{os.environ.get('AWS_LAMBDA_FUNCTION_NAME')}_config") or {})

        # Update config recursively from custom config
        self.config = recursive_update(self.config, custom_config)


    @benchmark
    def register_clients(self, clients):
        """
        Initialize the given `clients` and assign them to self with suffix `_client`.

        Clients are imported from the `components` or `managers` packages of your own Lambda, from
        `sosw.components`, or fall back to a plain boto3 client. Name of the module must be
        underscored name of Client. Name of the Class must be name of `client` with either of the
        suffixes ('Manager' or 'Client').

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
        ]

        # # Initialize required clients
        for service in clients:
            module_name = camel_case_to_underscore(service)

            for path in import_paths:
                try:
                    some_module = import_module(path(module_name))
                    logger.debug("Imported %s from %s",service, path(module_name))
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
                    logger.debug("Didn't find %s with suffix %s in module %s", service, suffix, module_name)
                    continue

                some_client_config = self.config.get(f"{module_name}_config")
                logger.debug("Found config for %s: %s", module_name, some_client_config)

                # Send configs one of the two ways as `config` or `custom_config` for some backwards compatibility
                if some_client_config:
                    if suffix == 'Manager':
                        setattr(self, f"{module_name}_client", some_class(custom_config=some_client_config))
                    elif suffix == 'Client':
                        setattr(self, f"{module_name}_client", some_class(config=some_client_config))

                else:
                    setattr(self, f"{module_name}_client", some_class())
                logger.info("Successfully registered %s_client", module_name)
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


    def _c(self, path: str, default: Any = None) -> Any | None:
        """
        Shortcut to access values from the Processor config.

        E.g. ``val = self._c('path.to.param')``

        Is similar to: ``val = self.config.get('path', {}).get('to', {}).get('param', default)``

        The value specified in `default` or None is returned if the path is not found.
        """
        result = recursive_matches_extract(self.config, path)
        return result if result is not None else default


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
                    logger.debug("Updated Processor stats with stats of %s", some_client)
                except Exception:
                    logger.debug("%s doesn't have get_stats() implemented. Recommended to fix this.", some_client)

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
    See the Processor examples in documentation for more info.
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


def _make_lambda_handler(processor_class, global_vars=None, custom_config=None):
    """
    Build the ``lambda_handler`` closure over the given Processor class.

    This is the shared internal builder used by :func:`get_lambda_handler` and by the optional
    durable functions wrapper (``sosw.durable.get_durable_lambda_handler``). It has no knowledge
    of any optional SDKs and must stay this way.

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

        try:
            if level := event.get('logging_level'):
                logger.setLevel(level)
        except AttributeError:
            logger.debug("The payload is not dict")
            pass

        logger.info("Called %s lambda of version %s with __name__: %s, context: %s",
                    os.environ.get('AWS_LAMBDA_FUNCTION_NAME'), os.environ.get('AWS_LAMBDA_FUNCTION_VERSION'),
                    __name__, context)
        logger.info(_log_redacted(event))

        test = _derive_test_flag(event.get('test') if isinstance(event, dict) else None)

        global_vars.lambda_context = context

        if global_vars.processor is None:
            global_vars.processor = processor_class(custom_config=custom_config, test=test)

        result = global_vars.processor(event)

        logger.info(global_vars.processor.get_stats())

        global_vars.processor.reset_stats(recursive=True)

        logger.info(_log_redacted(result))

        return result


    return lambda_handler


def get_lambda_handler(processor_class, global_vars=None, custom_config=None):
    """
    Return a reference to the entry point of the lambda function.

    The handler caches the initialized Processor in `global_vars` and reuses it in the warm
    invocations for the lifetime of the Lambda container. Per-invocation state belongs to
    ``processor.result`` (reset on every call), while ``processor.stats`` carries the counters
    of the container lifetime: ``reset_stats()`` runs once after every invocation and preserves
    the ``total_*`` counters and the ones configured in ``lifetime_stats_params``.

    The logged copies of the event and the result are redacted: values of keys whose lowercase name
    contains any substring from ``LOG_SENSITIVE_KEY_PARTS`` (e.g. ``Authorization``, ``Cookie``,
    ``X-Origin-Verify``) are replaced with ``LOG_REDACTED_VALUE`` — ``None``, booleans and numbers
    of counter-like keys (a whole word of ``LOG_COUNTER_KEY_WORDS``, e.g. ``max_tokens``) are kept.
    Sensitive parameters of ``rawQueryString``, secrets inside JSON ``body`` strings and
    form-encoded bodies are masked, and a body with a truthy sibling ``isBase64Encoded``, a
    ``multipart/`` body or one that looks like JSON but does not parse is replaced with the marker
    (base64 is reversible, and an unparsable payload may still carry secrets). The Processor still
    receives the original event, and the original result object is returned to the caller.

    :param processor_class:  Callable processor class.
    :param global_vars:      Lambda's global variables (processor, context).
    :param custom_config:    Custom configuration to pass the processor constructor.
    :return: Function reference for the lambda handler.
    """

    return _make_lambda_handler(processor_class, global_vars, custom_config)


# Global placeholder for global_vars.
global_vars = LambdaGlobals()
