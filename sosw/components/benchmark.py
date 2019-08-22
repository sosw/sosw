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
