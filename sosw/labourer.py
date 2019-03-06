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
    TIMESTAMPS = ('start', 'invoked', 'expired')
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


    def set_timestamp(self, name: str, value: int):
        """ Set timestamp attributes with some validation. Normally TaskManager is supposed to call me. """

        if name not in self.TIMESTAMPS:
            raise ValueError(f"Supported values are: {', '.join(self.TIMESTAMPS)}")
        print(f"Set {name, value}")
        setattr(self, name, value)


    def get_timestamp(self, name: str) -> int:
        """ The Labourer must be first registered in TaskManager for this to work. """

        if name not in self.TIMESTAMPS:
            raise ValueError(f"Supported values are: {', '.join(self.TIMESTAMPS)}")

        try:
            return getattr(self, name)
        except AttributeError:
            raise AttributeError(f"The Labourer is not yet registered in TaskManager, and doesn't have any timestamps. "
                                 f"Use TaskManager.register_labourer() first.")
