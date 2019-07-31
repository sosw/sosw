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

__all__ = ['benchmark']
__author__ = "Nikolay Grishchenko"
__version__ = "1.0"

import time


def benchmark(fn):
    """
    Decorator that should be used on class methods that you want to benchmark.
    It will aggregate to `self.stats` of the class timing of decorated functions.

    | `fn` - pointer to class function. Class is not yet initialized.
    | `self` - pointer to class instance. Passed during the call of decorated method.
    """


    def _timing_and_call_counter(self, *a, **kw):
        st = time.perf_counter()
        r = fn(self, *a, **kw)
        self.stats[f"time_{fn.__name__}"] += time.perf_counter() - st
        self.stats[f"calls_{fn.__name__}"] += 1
        return r


    return _timing_and_call_counter
