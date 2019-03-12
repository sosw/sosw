"""
Static helper methods which you can use in any Lambdas.
Must be completely independent with no specific requirements.
"""

__all__ = ['validate_account_to_dashed',
           'validate_account_to_int',
           'validate_list_of_numbers_from_csv',
           'camel_case_to_underscore',
           'chunks',
           'validate_uuid4',
           'rstrip_all',
           'get_one_or_none_from_dict',
           'get_one_from_dict',
           'get_list_of_multiple_or_one_or_empty_from_dict',
           'validate_date_list_from_event_or_days_back',
           'validate_date_from_something',
           'validate_datetime_from_something',
           'validate_string_matches_datetime_format',
           'recursive_matches_soft',
           'recursive_matches_strict',
           'recursive_matches_extract',
           'convert_string_to_words',
           'construct_dates_from_event',
           'validate_list_of_words_from_csv_or_list',
           'first_or_none'
           ]

import re
import uuid
import datetime
from typing import Iterable, Callable


def validate_account_to_dashed(account):
    """
    Validates the the provided string is in valid AdWords account format and converts it to dashed format.

    :param str account: AdWords Account
    :rtype:             str
    :return:            Dashed format
    """

    account = str(account).strip()
    if re.match("[0-9]{3}-[0-9]{3}-[0-9]{4}", account):
        return account
    elif re.match("^[0-9]{10}$", account):
        return '-'.join([str(account)[0:3], str(account)[3:6], str(account)[6:]])
    else:
        raise ValueError("Invalid account format provided: {}".format(account))


def validate_account_to_int(account):
    """
    Validates the the provided string is in valid AdWords account format and converts it to integer format.

    :param (str, int) account: AdWords Account
    :return:                   Account ID as integer
    """

    account = str(account).strip().replace('-', '')
    if re.match("^[0-9]{10}$", account):
        return int(account)
    else:
        raise ValueError("Invalid account format provided: {}".format(account))


def validate_list_of_numbers_from_csv(data):
    """
    Converts a comma separated string of numeric values to a list of sorted unique integers.
    The values that do not match are skipped.

    :param (str, iterable) data:    - str | iterable
    :return:        - list(int)
    """

    if isinstance(data, str):
        return [int(x.strip()) for x in data.split(',') if x.strip().isnumeric()]
    else:
        if isinstance(data, (int, float)):
            return [data]
        result = []
        try:
            for x in data:
                if isinstance(x, (int, float)):
                    result.append(int(x))
                elif isinstance(x, str) and x.strip().isnumeric():
                    result.append(int(x.strip()))
        except TypeError:
            pass
        return result


def validate_uuid4(uuid_string):
    """
    Validate that a UUID string is in
    fact a valid uuid4.
    Happily, the uuid module does the actual
    checking for us.
    It is vital that the 'version' kwarg be passed
    to the UUID() call, otherwise any 32-character
    hex string is considered valid.
    """

    try:
        val = uuid.UUID(uuid_string, version=4)
    except ValueError:
        # If it's a value error, then the string
        # is not a valid hex code for a UUID.
        return False

    # If the uuid_string is a valid hex code,
    # but an invalid uuid4,
    # the UUID.__init__ will convert it to a
    # valid uuid4. This is bad for validation purposes.


def camel_case_to_underscore(name):
    """
    Converts attribute to string and formats it as underscored.

    :param name:    - str   -   CamelCase string (or something convertable to CamelCase with __str__() method.
    :return:        - str   -   underscore_formatted_value
    """

    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', str(name))
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def chunks(l, n):
    """Yield successive n-sized chunks from l."""

    for i in range(0, len(l), n):
        yield l[i:i + n]


def rstrip_all(input, patterns):
    """
    Strips all of the patterns from the right of the input. Order and spaces do not matter.

    :param input:       - str                   - String to modify
    :param patterns:    - list|set|tuple|str    - Pattern[-s] to remove.
    :return:            - str
    """

    if isinstance(patterns, str):
        regex = re.compile("({}$)".format(re.escape(patterns)))
    else:
        regex = re.compile("({}$)".format("$|".join(map(re.escape, patterns))))

    if isinstance(patterns, str):
        patterns = [patterns]
    if not isinstance(patterns, (list, set, tuple)) or not all(isinstance(x, str) for x in patterns):
        raise ValueError("Patterns for rstrip_all() are supposed to be string or iterable of strings")
    rabbit = input.strip()
    r = regex.sub('', rabbit)
    if not r == rabbit:
        # Go recursive in case we stripped smth in this iteration.
        return rstrip_all(r, patterns)
    else:
        # If nothing left to change, return the rabbit.
        return rabbit


def get_one_or_none_from_dict(input, name, vtype=None):
    """
    Extracts object by 'name' from the 'input'.
    Tries also plural name in case not found by single 'name'.
    In case found an iterable by plural name, validates that it has one or zero values in it.
    If vtype is specified, tries to convert result to it.

    :param dict input:  Input dictionary. Event of Lambda for example.
    :param str name:    Name of attribute (in singular form).
    :param type vtype:  Type to be converted to. Must be callable. Tested types: str, int, float
    :return:            - instance of vtype | something else | None

    :raises ValueError: In all cases something is wrong.
    """

    if not isinstance(input, dict):
        raise ValueError("'input' attribute must be a dict. Received: {}".format(type(input)))

    if not isinstance(name, str):
        raise ValueError("'name' attribute must be a str. Received: {}".format(type(name)))


    def convert(obj, t):
        return obj if not t else t(obj)


    # Best case scenario. :)
    result = input.get(name)
    if result:
        return convert(result, vtype)

    # if not result, try to search for plural
    results = input.get(name + 's')
    if not results:
        return None

    # If we found some results with plural name, we make sure it is iterable and has one or zero value.
    if isinstance(results, (list, tuple, set)):
        if len(results) > 1:
            raise ValueError("More than one {}s found in input.".format(name))
        else:
            return convert(results[0], vtype)
    elif results:
        raise ValueError("Some not-iterable '{}s' found in input: {}".format(name, str(type(result))))


def get_one_from_dict(input, name, vtype=None):
    """
    Extracts object by 'name' from the 'input'.
    Tries also plural name in case not found by single 'name'.
    In case found an iterable by plural name, validates that it has exactly one value in it.
    If vtype is specified, tries to convert result to it.

    :param input:       - dict       - Input dictionary. Event of Lambda for example.
    :param name:        - str        - Name of attribute (in singular form).
    :param vtype:       - type       - Type to be converted to. Must be callable. Tested types: str, int, float
    :return:            - instance of vtype | something else | None
    :raises ValueError:  - In all cases something is wrong.
    """

    result = get_one_or_none_from_dict(input, name, vtype)
    if result:
        return result
    else:
        raise ValueError("Did not find any value {} in the input {}".format(name, input))


def get_list_of_multiple_or_one_or_empty_from_dict(input, name, vtype=None):
    """
    Extracts objects by 'name' from the 'input' and returns as a list.
    Tries both plural and singular names from the input.
    If vtype is specified, tries to convert each of the elements in the result to this type.

    :param input:       - dict       - Input dictionary. Event of Lambda for example.
    :param name:        - str        - Name of attribute (in plural form).
    :param vtype:       - type       - Type to be converted to. Must be callable. Tested types: str, int, float
    :return:            - list       - List of vtypes, or list of whatever was in input, or empty list.
    :raises ValueError:  In all cases something is wrong.
    """

    if not isinstance(input, dict):
        raise ValueError("'input' attribute must be a dict. Received: {}".format(type(input)))

    if not isinstance(name, str):
        raise ValueError("'name' attribute must be a str. Received: {}".format(type(name)))


    def convert(obj, t):
        return obj if not t else t(obj)


    results = input.get(name) or input.get(name.rstrip('s'))
    if not results:
        return []

    # Wrap to list if not there yet.
    if not isinstance(results, (list, tuple, set)):
        results = [results]
    else:
        results = list(results)

    # Apply vtype convertion if required.
    return [convert(x, vtype) for x in results]


def validate_date_list_from_event_or_days_back(input, days_back=0, key_name='date_list'):
    """
    Takes from input the date_list and extracts date_list. Validates and converts to datetime.date.
    Input should have date_list as list of strings or comma-separated string.

    * Format:     ``YYYY-MM-DD``
    * Examples:

    .. code-block:: python

       ['2018-01-01', '2018-02-01']
       '2018-01-01, 2018-02-01'

    :param dict input:      This is supposed to be your whole Lambda event.
    :param int days_back:   Optional Number of days to take back from today.
                            Ex: days_back=1 is yesterday. Default: today.
    :param str key_name:    Optional custom name of key to extract from 'input'.
    :return:                list(datetime.date)
    """

    date_list = input.get(key_name, '')

    if not date_list:
        return [datetime.date.today() - datetime.timedelta(days=days_back)]

    if not isinstance(date_list, (list, set, tuple)):
        date_list = str(date_list).split(',')
    return [datetime.datetime.strptime(x.strip(), '%Y-%m-%d').date() for x in date_list]


def validate_datetime_from_something(d):
    """
    Converts the input `d` to datetime.datetime.

    :param d: Some input. Supported types:
                * datetime.datetime
                * datetime.date
                * int - Epoch or Epoch milliseconds
                * float - Epoch or Epoch milliseconds
                * str (YYYY-MM-DD)
                * str (YYYY-MM-DD HH:MM:SS)
    :return: Transformed `d`
    :rtype: datetime.datetime
    :raises: ValueError
    """

    mutators = [
        (datetime.datetime, lambda x: x),
        (datetime.date, lambda x: datetime.datetime.combine(x, datetime.datetime.min.time())),
        ((int, float), lambda x: datetime.datetime.fromtimestamp(x)
        if x < datetime.datetime(datetime.MAXYEAR, 12, 31).timestamp()
        else datetime.datetime.fromtimestamp(x / 1000)),
        (str, lambda x: datetime.datetime.strptime(d, '%Y-%m-%d')
        if len(d) == 10 else datetime.datetime.strptime(d[:19], '%Y-%m-%d %H:%M:%S'))
    ]

    for mutator in mutators:
        if isinstance(d, mutator[0]):
            return mutator[1](d)

    raise ValueError("Some unconvertable type for datetime validation: {}".format(d))


def validate_date_from_something(d):
    """
    Convert valid input to datetime.date() or raise either AttributeError or ValueError.

    :param d: Some input. Supported types:
                * datetime.datetime
                * datetime.date
                * int - Epoch or Epoch milliseconds
                * float - Epoch or Epoch milliseconds
                * str (YYYY-MM-DD)
                * str (YYYY-MM-DD HH:MM:SS)
    :return: Transformed `d`
    :rtype: datetime.date
    :raises: ValueError
    """

    return validate_datetime_from_something(d).date()


def validate_string_matches_datetime_format(date_str, date_format, field_name='date'):
    """
    Validate string, make sure it's of the given datetime format

    :param str date_str: a date or time or both, Example: '2018/09/16'
    :param str date_format: datetime format, that is acceptable for datetime.strptime. Example: '%Y/%m/%d'
            (https://docs.python.org/3.6/library/datetime.html#strftime-and-strptime-behavior)
    :param str field_name: name of the field (for the error)
    :raises: ValueError
    """

    if not (date_str and isinstance(date_str, str)):
        raise ValueError(f"Bad input for {field_name} ({date_str}), must be a string")
    if not (date_format and isinstance(date_format, str)):
        raise ValueError(f"Bad input for format ({date_format}), must be a string")

    try:
        datetime.datetime.strptime(date_str, date_format)
    except ValueError:
        raise ValueError(f"Incorrect format of {field_name} ({date_str}), should be {date_format}")


def recursive_matches_soft(src, key, val, **kwargs):
    """
    Searches the 'src' recursively for nested elements provided in 'key' with dot notation.
    In case some levels are iterable (list, tuple) it checks every element.
    In case the full path is inaccessible returns False.
    If any of the elements addressed by 'key' matches the 'val' - Bingo! Return True.

    You might also be interested in recursive_exists_strict() helper.

    :param dict src:    Input dictionary. Can contain nested dictionaries and lists.
    :param str key:     Path to search with dot notation.
    :param any val:     Value to match in some elements specified by path.

    In order to check not just that some element exists, but to check for duplicates, you might want to use
    optional 'exclude' attributes. If attributes are specified and the last level element following the path
    (dot notation) will have a key-value, the check for the main key-value will be skipped.
    See unittests to understand the bahaviour better.

    :param str exclude_key:     Key to check in last level element to exclude.
    :param srt exclude_val:     Value to match in last level element to exclude.

    :rtype: bool
    """

    if any([x in kwargs for x in ['exclude_key', 'exclude_val']]) \
            and not all([x in kwargs for x in ['exclude_key', 'exclude_val']]):
        raise AttributeError("If you use 'exclude' attributes you must specify both 'exclude_key' and 'exclude_val'")

    path_elements = key.split('.')
    # logging.debug("Invoked func: ", src, key, path_elements)

    # if src is iterable: iterate recursively
    if isinstance(src, (list, tuple)):
        return any(recursive_matches_soft(element, key, val, **kwargs) for element in src)

    # We should try to dig deeper.
    elif len(path_elements) > 1:
        try:
            if recursive_matches_soft(src[path_elements[0]], '.'.join(path_elements[1:]), val, **kwargs):
                return True
        except KeyError:
            pass

    # Last level of digging
    elif len(path_elements) == 1:
        try:
            if kwargs.get('exclude_key') and src[kwargs['exclude_key']] == kwargs['exclude_val']:
                # logging.debug("Skipping element because it matches exclude parameters.")
                return False
        except KeyError:
            pass  # There is a chance that the exclude key is simply missing. We ignore it then.
        try:
            return src[key] == val
        except (KeyError, TypeError):
            pass
    else:
        raise RuntimeError("Your function is stupid")

    # If nothing found we return False
    return False


def recursive_matches_strict(src, key, val, **kwargs):
    """
    Searches the 'input' recursively for nested elements provided in 'key' with dot notation.
    In case some levels are iterable (list, tuple) it checks every element.
    In case the full path is inaccessible raises AttributeError or KeyError.

    :param dict src:    Input dictionary. Can contain nested dictionaries and lists.
    :param str key:     Path to search with dot notation.
    :param any val:     Value to match in some elements specified by path.

    :rtype: bool
    """

    if any([x in kwargs for x in ['exclude_key', 'exclude_val']]) \
            and not all([x in kwargs for x in ['exclude_key', 'exclude_val']]):
        raise AttributeError("If you use 'exclude' attributes you must specify both 'exclude_key' and 'exclude_val'")

    path_elements = key.split('.')

    # if src is iterable: iterate
    if isinstance(src, (list, tuple)):
        return any(recursive_matches_strict(x, key, val, **kwargs) for x in src)
    elif len(path_elements) > 1:
        return recursive_matches_strict(src[path_elements[0]], '.'.join(path_elements[1:]), val, **kwargs)
    elif len(path_elements) == 1:
        try:
            if kwargs.get('exclude_key') and src[kwargs['exclude_key']] == kwargs['exclude_val']:
                # logging.debug("Skipping element because it matches exclude parameters.")
                return False
        except KeyError:
            pass  # There is a chance that the exclude key is simply missing. We ignore it then.
        return src[key] == val
    else:
        raise RuntimeError("Your function is stupid", src, key, val)


def recursive_matches_extract(src, key, **kwargs):
    """
    Searches the 'src' recursively for nested elements provided in 'key' with dot notation.
    In case some levels are iterable (list, tuple) it checks every element in it till finds it.

    Returns the first found element or None.
    In case the full path is inaccessible also returns None.

    If you are just checking if some elements exist, you might be interested in
    recursive_exists_strict() or recursive_exists_soft() helpers.

    .. :warninig:
        Please be aware that this method doesn't not check for duplicates in iterable elements on neither
        level during extraction.

    :param dict src:    Input dictionary. Can contain nested dictionaries and lists.
    :param str key:     Path to search with dot notation.

    In order to filter out some specific elements, you might want to use the optional 'exclude' attributes.
    If attributes are specified and the last level element following the path
    (dot notation) will have a key-value, the check for the main key-value will be skipped.
    See unittests to understand the bahaviour better.

    :param str exclude_key:     Key to check in last level element to exclude.
    :param str exclude_val:     Value to match in last level element to exclude.

    :return:    Value from structure extracted by specified path
    """

    if any([x in kwargs for x in ['exclude_key', 'exclude_val']]) \
            and not all([x in kwargs for x in ['exclude_key', 'exclude_val']]):
        raise AttributeError("If you use 'exclude' attributes you must specify both 'exclude_key' and 'exclude_val'")

    path_elements = key.split('.')
    # logging.debug("Invoked func: ", src, key, path_elements)

    # if src is iterable: iterate recursively
    if isinstance(src, (list, tuple)):
        for element in src:
            v = recursive_matches_extract(element, key, **kwargs)
            if v:
                return v

    # We should try to dig deeper.
    elif len(path_elements) > 1:
        try:
            return recursive_matches_extract(src[path_elements[0]], '.'.join(path_elements[1:]), **kwargs)
        except KeyError:
            pass

    # Last level of digging
    elif len(path_elements) == 1:
        try:
            if kwargs.get('exclude_key') and src[kwargs['exclude_key']] == kwargs['exclude_val']:
                # logging.debug("Skipping element because it matches exclude parameters.")
                return None
        except KeyError:
            pass  # There is a chance that the exclude key is simply missing. We ignore it then.
        return src.get(key)
    else:
        raise RuntimeError("Your function is stupid")

    # If nothing found we return False
    return None


def convert_string_to_words(string):
    """
    Convert string to comma separated words.

    :param  str string:     String to convert into words.
    :rtype: str
    :return: Comma separated words.
    """

    if not isinstance(string, str):
        raise TypeError(f"Input must be string, got {type(string)}")

    return re.sub('\s+', ',', string.lower().strip())


def construct_dates_from_event(event: dict) -> tuple:
    """
    Processes given event dictionary for start and end points of time. Otherwise takes the default settings.

    The end date of the period may be specified as `en_date` in the event. The default value is today.

    Also the `event` should have either `st_date` or `days_back` numeric parameter.
    If provided the days_back it will be substracted from end date.

    Both `st_date` and `en_date` might be either `date`, `datetime` or `string` (`'YYYY-MM-DD'`) types.
    In case of `datetime`, the hours/minutes/etc are ignored.

    :param dict event:  Lambda payload.
    :return:            start_date, end_date    as datetime.date
    """

    en_date = validate_date_from_something(event.get('en_date', datetime.date.today()))
    st_date = event.get('st_date')
    days_back = event.get('days_back')

    if st_date and days_back:
        raise AttributeError(f"construct_dates_from_event() doesn't allow st_date and days_back simultaneously")

    if not st_date and not days_back:
        raise AttributeError(f"construct_dates_from_event() expects either st_date or days_back")

    if days_back:
        st_date = en_date - datetime.timedelta(days=int(days_back))
    else:
        st_date = validate_date_from_something(st_date)
        assert st_date < en_date, "Start date must be earlier than end date."

    return st_date, en_date


def validate_list_of_words_from_csv_or_list(data: (str, list)) -> list:
    """
    Splits a CSV string to list of stripped words.
    In case the `data` is already a list of strings - splits it's elements and flattens the result.

    All resulting elements must be single words, if any of the elements contains spaces (i.e. multiple words)
    the validation fails with `ValueError`.

    :param data:    CSV string of list of strings (possibly CSV themselves)
    :return:        List of stripped and split words
    """


    def split_csv(row):
        if not isinstance(row, str):
            raise TypeError(f"Unsupported type of data for validate_list_of_words_from_csv_or_list(): {data}")

        return [x.strip() for x in row.split(',')]


    result = []
    if isinstance(data, (list, tuple, set)):
        for element in data:
            result.extend(split_csv(element))
    else:
        result = split_csv(data)

    if any(' ' in x for x in result):
        raise ValueError(f"data for validate_list_of_words_from_csv_or_list() should be csv of WORDS or list: {data}")

    return result


def first_or_none(items: Iterable, condition: Callable = None):
    """
    Return first element in iterable to match condition or None
    """

    if not condition:
        def condition(*args, **kwargs):
            return True

    for item in items:
        if condition(item):
            return item

    return None
