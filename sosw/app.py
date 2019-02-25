import boto3
import logging
import os

from importlib import import_module
from collections import defaultdict

from sosw.components.helpers import *
from sosw.components.config import get_config


__author__ = "Nikolay Grishchenko"
__email__ = "dev@bimpression.com"
__version__ = "0.3.1"
__license__ = "MIT"
__status__ = "Production"


__all__ = ['Processor']

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Processor:
    """
    Core Processor class template.
    """

    DEFAULT_CONFIG = {}


    def __init__(self, custom_config=None, **kwargs):
        """
        Initialize the Processor.
        Updates the default config with parameters from SSM, then from provided custom config (usually from event).
        """

        self.test = kwargs.get('test') or True if os.environ.get('STAGE') in ['test', 'autotest'] else False

        if self.test and not custom_config:
            raise RuntimeError("You must specify a custom config from your testcase to run processor in test mode.")

        self.config = self.DEFAULT_CONFIG.copy()
        self.config.update(self.get_config(f"{os.environ.get('AWS_LAMBDA_FUNCTION_NAME')}_config"))
        self.config.update(custom_config or {})
        logger.info(f"Final processor config: {self.config}")

        self.stats = defaultdict(int)

        self.register_clients(self.config.get('init_clients', []))


    def register_clients(self, clients):
        """
        Initialize the given `clients` and assign them to self with suffix `_client`.

        Clients are imported from `components`. Name of the module must be underscored name of Client.
        Name of the Class must be name of `client` with either of the suffixes ('Manager' or 'Client').

        .. warning::
           To be implemented!

           If you follow these rules and put the module in package `components` of your Lambda,
           you can just provide the `clients` in custom_config when initializing the Processor.

        :param list clients:    List of names of clients.
        """

        client_suffixes = ['Manager', 'Client']

        # # Initialize required clients
        for service in clients:
            module_name = camel_case_to_underscore(service)
            try:
                some_module = import_module(f"sosw.components.{module_name}")
            except:
                # Also try to import from the components directory owned by the Lambda itself.
                try:
                    some_module = import_module(f"components.{module_name}")
                except:

                    # The other supported option is to load boto3 client if it exists.
                    try:
                        setattr(self, f"{module_name}_client", boto3.client(module_name))
                        continue
                    except:
                        logger.error(f"Failed to import module for service {module_name}. Component naming problem.")
                        raise RuntimeError(f"Failed to import for service {module_name}. Component naming problem.")

            for suffix in client_suffixes:
                try:
                    some_class = getattr(some_module, f"{service}{suffix}")
                    some_client_config = self.config.get(f"{module_name}_config")
                    if some_client_config:
                        setattr(self, f"{module_name}_client", some_class(config=some_client_config))
                    else:
                        setattr(self, f"{module_name}_client", some_class())
                    logger.info(f"Successfully registered {module_name}_client")
                    break
                except:
                    pass
            else:
                raise RuntimeError(f"Failed to import {service} from {some_module}. "
                                   f"Tried suffixes for class: {client_suffixes}")


    def __call__(self, event):
        """
        Call the Processor.
        You can either call super() at the end of your child function or completely overwrite this function.
        """

        # Update the stats for number of calls.
        # Makes sense for Processors initialized outside the scope of `lambda_handler`.
        self.stats['processor_calls'] += 1


    @staticmethod
    def get_config(name):
        """
        Returns config by name from SSM. Override this to provide your config handling method.

        :param name: Name of the config
        :rtype: dict
        """

        return get_config(name)


    def get_stats(self):
        """
        Return statistics of operations performed by current instance of the Class.

        Statistics of custom clients existing in the Processor is also aggregated.
        Clients must be initialized as `self.some_client` ending with `_client` suffix (e.g. self.dynamo_client).
        Clients must also have their own get_stats() methods implemented.

        :rtype:     dict
        :return:    Statistics counter of current Processor instance.
        """

        for some_client in [x for x in dir(self) if x.endswith('_client')]:
            try:
                self.stats.update(getattr(self, some_client).get_stats())
                logger.info(f"Updated Processor stats with stats of {some_client}")
            except:
                logger.warning(f"{some_client} doesn't have get_stats() implemented. Recommended to fix this.")

        return self.stats


    def reset_stats(self):
        """
        Cleans statistics other than specified for the lifetime of processor.
        All the parameters with prefix *'total_'* are also preserved.

        The function makes sense if your Processor lives outside the scope of `lambda_handler`.
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

        for some_client in [x for x in dir(self) if x.endswith('_client')]:
            try:
                getattr(self, some_client).reset_stats()
            except:
                pass


    def die(self, message="Unknown Failure"):
        """
        Logs current Processor stats and `message`. Then raises RuntimeError with `message`.

        :param str message:
        """

        logger.exception(message)

        result = {'status': 'failed'}
        result.update(self.get_stats())
        logger.info(result)

        raise RuntimeError(message)



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
        except:
            pass

        try:
            self.conn.close()
        except:
            pass
