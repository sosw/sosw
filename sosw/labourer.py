"""
This class represents types of Workers.

We use a different name in order to avoid any possible mess with the class Worker (which the actual Workers
should inherit from). This one is just a set of settings and common methods for the type of Labourers.
"""

__all__ = ['Labourer']
__author__ = "Nikolay Grishchenko"

import logging
import time


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Labourer:
    ATTRIBUTES = ('id', 'arn')
    CUSTOM_ATTRIBUTES = ('start', 'invoked', 'expired', 'health', 'max_attempts', 'average_duration', 'max_duration')
    id = None
    arn = None

    DEFAULTS = {
        'duration': 900,  # 15 minutes.
        'cooldown': 300,  # 5 minutes.
    }


    def __init__(self, **kwargs):

        if kwargs.pop('strict', False):
            for k, v in kwargs.items():
                if k in self.ATTRIBUTES:
                    setattr(self, k, v)
                else:
                    raise AttributeError(f"Not supported attribute for Labourer: {k}")
        else:
            for k, v in kwargs.items():
                setattr(self, k, v)

        self.set_defaults()


    def set_defaults(self):
        """
        Set some default values.
        You may (and should) overwrite them for your Labourers in config of the Orchestrator.
        """

        for k, v in self.DEFAULTS.items():
            if not getattr(self, k, None):
                setattr(self, k, v)


    def set_custom_attribute(self, name: str, value: int):
        """ Set timestamp attributes with some validation. Normally TaskManager is supposed to call me. """

        if name not in self.CUSTOM_ATTRIBUTES:
            raise ValueError(f"Supported values are: {', '.join(self.CUSTOM_ATTRIBUTES)}")
        logger.debug(f"Labourer {self.id} set custom attribute {name} with {value}")
        setattr(self, name, value)


    def get_attr(self, name: str):
        """ The Labourer must be first registered in TaskManager for this to work. """

        if name not in self.CUSTOM_ATTRIBUTES:
            raise ValueError(f"Supported values are: {', '.join(self.CUSTOM_ATTRIBUTES)}")

        try:
            return getattr(self, name)
        except AttributeError:
            raise AttributeError(f"The Labourer is not yet registered in TaskManager, and doesn't have any custom "
                                 f"attributes. Use TaskManager.register_labourer() first.")
