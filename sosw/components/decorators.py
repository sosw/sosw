"""
..  hidden-code-block:: text
    :label: View Licence Agreement <br>

    sosw - Serverless Orchestrator of Serverless Workers
    Copyright (C) 2019  sosw core contributors

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/gpl-3.0.html>.
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
