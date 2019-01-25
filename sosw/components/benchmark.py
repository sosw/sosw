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


    def _timing(self, *a, **kw):
        st = time.perf_counter()
        r = fn(self, *a, **kw)
        self.stats[f"time_{fn.__name__}"] += time.perf_counter() - st
        return r


    return _timing
