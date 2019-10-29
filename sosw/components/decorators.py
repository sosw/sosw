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

import logging


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
