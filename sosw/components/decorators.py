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
            except:
                logging.log(level, f"Running {method.__name__} with args={args}, kwargs={kwargs}")

            result = method(*args, **kwargs)
            return result
        return wrapper
    return decorator
