"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - Serverless Orchestrator of Serverless Workers

    The MIT License (MIT)
    Copyright (C) 2020  sosw core contributors <info@sosw.app>

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

__all__ = ['Essential']
__author__ = "Mark Bulgakov"
__version__ = "1.0"

import logging
import os

from sosw.app import Processor
from sosw.components.helpers import recursive_update
from sosw.managers.meta_handler import MetaHandler


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Essential(Processor):
    """
    This class abstracts common base properties and methods shared by essential Lambda functions.

    Currently implemented:

    * Update the ``self.config`` with shared settings (e.g list of registered Labourers)
    """

    meta_handler: MetaHandler = None


    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.meta_handler = MetaHandler(custom_config=self.config.get('meta_handler_config'))


    def init_config(self, custom_config=None):
        """
        Overwritten parent method.
        We expect to receive essential config first, after that all the updates should be done
        """

        # Initialize config from essential config
        self.config = self.get_config("sosw_essential_config") or {}
        #  # Update config recursively from DEFAULT_CONFIG
        self.config = recursive_update(self.config, self.DEFAULT_CONFIG)
        # Update config recursively from any existing lambda function config
        self.config = recursive_update(self.config,
                                       self.get_config(f"{os.environ.get('AWS_LAMBDA_FUNCTION_NAME')}_config") or {})
        # Update config recursively from custom config
        self.config = recursive_update(self.config, custom_config or {})
