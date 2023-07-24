from typing import Union, Optional
from datetime import datetime, date  # noqa: I251

from dlt.common import pendulum
from dlt.common.time import parse_iso_like_datetime


TAnyDateTime = Union[pendulum.DateTime, pendulum.Date, datetime, date, str]


def ensure_pendulum_datetime(value: TAnyDateTime) -> pendulum.DateTime:
    """Coerce a date/time value to a `pendulum.DateTime` object.

    UTC is assumed if the value is not timezone aware.

    Args:
        value: The value to coerce. Can be a pendulum.DateTime, pendulum.Date, datetime, date or iso date/time str.

    Returns:
        A timezone aware pendulum.DateTime object.
    """
    if isinstance(value, datetime):
        # both py datetime and pendulum datetime are handled here
        ret = pendulum.instance(value)
        if ret.tz is None:
            return ret.in_tz("UTC")
        return ret
    elif isinstance(value, date):
        return pendulum.datetime(value.year, value.month, value.day)
    elif isinstance(value, str):
        result = parse_iso_like_datetime(value)
        if not isinstance(result, datetime):  # TODO: iso date parses to date object
            return pendulum.datetime(result.year, result.month, result.day)
        return result
    raise TypeError(f"Cannot coerce {value} to a pendulum.DateTime object.")
