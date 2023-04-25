def hello(name, age, tags=None):
    """
    User friendly welcome function.
    Uses `name` and `age` to salute user.
    This is still same line of documentation.

    While this is a new paragraph.
    Note that `rst` is sensitive to empty lines and spaces.

    Some code Example:

    .. code-block:: python

        def hello():
            return "world"

    This is paragraph 3.

    * And some bullet list
    * With couple rows
    * Even 3 rows

    Now go parameters. PyCharm adds them automatically.

    :param str name:    User name of type string.
    :param tags:        Types are not required, but this is a good
                        practice to show what you expect.

    :param age:         You can also specify multiple types, with a
                        little different syntax.
                        Note that empty lines do not matter here for
                        `sphinx`, but good for code readability.
    :type age:          int | float

    :rtype:             dict
    :return:            You can specify type of return value in
                        `rtype` (if it is uncertain).
    """


    return f"Hello {'bro' if age > 10 else 'kid'}"



