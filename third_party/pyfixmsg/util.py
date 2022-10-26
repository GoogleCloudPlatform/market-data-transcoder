"""Small utility-type functions"""

# pylint: disable=invalid-name

import datetime

import six

DATEFORMAT = '%Y%m%d-%H:%M:%S.%f'


def int_or_str(val, decode_as=None):
    """ simple format to int or string if not possible """
    try:
        return int(val)
    except ValueError as exc:
        if decode_as is None:
            if isinstance(val, (bytes, six.text_type)):
                return val.strip()
            return str(val)
        if isinstance(val, bytes):
            return val.decode(decode_as).strip()
        raise ValueError(f'Cannot decode type {type(val)}') from exc


def native_str(val, encoding='UTF-8'):
    """ format to native string (support int type) """
    if val is None:
        return val
    if isinstance(val, int):
        return str(val)
    try:
        return six.ensure_str(val, encoding=encoding)
    except TypeError:
        return str(val)  # i.e. val is Decimal type


def utc_timestamp():
    """
    @return: a UTCTimestamp (see FIX spec)
    @rtype: C{str}
    """
    return datetime.datetime.utcnow().strftime(DATEFORMAT)
