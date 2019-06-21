__all__ = ['extract_call_params', 'line_count']

import inspect
import subprocess

from typing import Dict


def extract_call_params(call_args, function) -> Dict:
    """
    Combines a dictionary out of Mock.call_args.
    Helper useful to validate specific call parameters of the Mocked function.

    When you are not sure if your function is called with args or kwargs, you just feed the call_args
    and the source of function to this helper and receive a dictionary.

    .. _code-block:: python

       call_kwargs = extract_call_params(your_mock_object.some_method.call_args, mocked_module.Class.method)

    :param call_args:   call_args of Mock object
    :param function:    Source object that was initially mocked
    :rtype:             dict
    """

    # Specification of arguments of function
    function_args = inspect.getfullargspec(function).args

    # Mock call_arguments as a tuple
    call_args, call_kwargs = call_args

    result = {}

    for i, v in enumerate(call_args):
        position = i + 1 if function_args[0] == 'self' else i
        result[function_args[position]] = v

    for k, v in call_kwargs.items():
        result[k] = v

    return result


def line_count(file):
    try:
        # This is the FAST way
        return int(subprocess.check_output(f'wc -l {file}', shell=True).split()[0])
    except Exception:
        # This is in case you are running in some creepy environment without shell access
        i = 0
        with open(file, 'r') as f:
            for i, _ in enumerate(f):
                pass
        return i + 1
