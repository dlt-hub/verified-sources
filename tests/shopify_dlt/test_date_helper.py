import pytest
from datetime import datetime, date, timezone  # noqa: I251
from dlt.common import pendulum

from sources.shopify_dlt.date_helper import ensure_pendulum_datetime, TAnyDateTime


test_params = [
    # python datetime without tz
    (
        datetime(2021, 1, 1, 0, 0, 0),
        pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
    ),
    # python datetime with tz
    (
        datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
    ),
    # python date object
    (date(2021, 1, 1), pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC")),
    # pendulum datetime with tz
    (
        pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
        pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
    ),
    # pendulum datetime without tz
    (
        pendulum.datetime(2021, 1, 1, 0, 0, 0),
        pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC"),
    ),
    # iso datetime in UTC
    ("2021-01-01T00:00:00+00:00", pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC")),
    # iso datetime with non utc tz
    (
        "2021-01-01T00:00:00+05:00",
        pendulum.datetime(2021, 1, 1, 0, 0, 0, tz=5),
    ),
    # iso datetime without tz
    (
        "2021-01-01T05:02:32",
        pendulum.datetime(2021, 1, 1, 5, 2, 32).in_tz("UTC"),
    ),
    # iso date
    ("2021-01-01", pendulum.datetime(2021, 1, 1, 0, 0, 0).in_tz("UTC")),
]


@pytest.mark.parametrize("date_value, expected", test_params)
def test_ensure_pendulum_datetime(
    date_value: TAnyDateTime, expected: pendulum.DateTime
) -> None:
    assert ensure_pendulum_datetime(date_value) == expected
