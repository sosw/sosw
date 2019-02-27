"""
This class represents types of Workers.

We use a different name in order to avoid any possible mess with the class Worker (which the actual Workers
should inherit from). This one is just a set of settings and common methods for the type of Labourers.
"""

__all__ = ['Labourer']
__author__ = "Nikolay Grishchenko"

import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Labourer:

    ATTRIBUTES = ('id', 'arn')
    id = None
    arn = None


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
