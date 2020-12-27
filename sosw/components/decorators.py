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

import logging
import time

from typing import Tuple
from functools import wraps


def logging_wrapper(level: int = None):
    """
    This wrapper will print the method name and the parameters you called the method with.

    Usage example:

    .. code-block:: python

        @logging_wrapper(logging.INFO)
        def foo(a, b, c=4, *arglist, **keywords): pass

    Running foo(1, 2, 3, 4, 5, t=6, z=7) will print: "Running foo with a=1, b=2, c=3, 4, 5, t=6, z=7"

    :param level: logging level, E.g. logging.INFO
    """

    if not level:
        level = logging.INFO

    def decorator(method):
        def wrapper(*args, **kwargs):
            try:
                var_names = method.__code__.co_varnames[:method.__code__.co_argcount]
                s_parts = [f"Running {method.__qualname__}"]

                if args or kwargs:
                    s_parts.append("with")

                for i, arg in enumerate(args):
                    if i < len(var_names):
                        if var_names[i] != "self":
                            s_parts.append(f"{var_names[i]}={arg},")
                    else:
                        s_parts.append(f"{arg},")  # For args without names

                for k, v in kwargs.items():
                    s_parts.append(f"{k}={v},")

                logging.log(level, " ".join(s_parts).strip(','))
            except Exception:
                logging.log(level, f"Running {method.__name__} with args={args}, kwargs={kwargs}")

            result = method(*args, **kwargs)
            return result
        return wrapper
    return decorator


def retry(exception_to_check: (Exception, Tuple[Exception]) = Exception, tries: int = 4, delay: int = 3,
          backoff: int = 2):
    """
    Retry calling the decorated function using an exponential backoff.
    Receive amount of retries and time in seconds of a delay between each retry.
    Based on: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param exception_to_check: The exception to check or a tuple of exceptions.
    :param tries: Number of tries
    :param delay: Delay between retries in seconds
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    """

    assert tries > 0, "Tries must be 1 or greater"
    assert delay >= 0, "Delay must be greater than 0"
    assert backoff >= 1, "Backoff must be greater than 1"


    def decorator_retry(func):
        @wraps(func)
        def func_retry(*args, **kwargs):
            mutable_tries, mutable_delay = tries, delay

            while mutable_tries > 1:
                try:
                    return func(*args, **kwargs)
                except exception_to_check as e:
                    msg = f"{str(e)}, Retrying in {mutable_delay} seconds..."
                    if logging:
                        logging.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mutable_delay)
                    mutable_tries -= 1
                    mutable_delay *= backoff

            return func(*args, **kwargs)

        return func_retry

    return decorator_retry
