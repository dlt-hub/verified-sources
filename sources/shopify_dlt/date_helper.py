from typing import Union, Optional
from datetime import datetime, date  # noqa: I251

from dlt.common import pendulum


TAnyDateTime = Union[pendulum.DateTime, pendulum.Date, datetime, date, str]


def ensure_pendulum_datetime(value: TAnyDateTime) -> pendulum.DateTime:
    """Coerce a value to a pendulum.DateTime object."""
    if isinstance(value, datetime):
        # both py datetime and pendulum datetime are handled here
        return pendulum.instance(value)
    elif isinstance(value, date):
        return pendulum.DateTime(value.year, value.month, value.day)
    elif isinstance(value, str):
        return pendulum.parse(value)  # type: ignore[return-value]
    raise TypeError(f"Cannot coerce {value} to a pendulum.DateTime object.")
