from typing import Any, Dict, Generator, Optional, Union

import stripe
from dlt.common import pendulum
from pendulum import DateTime


def pagination(
    endpoint: str,
    start_date: Optional[Any] = None,
    end_date: Optional[Any] = None
) -> Generator[Dict[Any, Any], Any, None]:
    get_more = True
    starting_after = None
    while get_more:
        response = stripe_get_data(
            endpoint,
            start_date=start_date,
            end_date=end_date,
            starting_after=starting_after,
        )
        get_more = response["has_more"]

        if len(response["data"]) > 0:
            starting_after = response["data"][-1]["id"]

        yield response["data"]


def transform_date(date: Union[str, DateTime, int]) -> int:
    if isinstance(date, str):
        date = pendulum.from_format(date, "%Y-%m-%dT%H:%M:%SZ")
    if isinstance(date, DateTime):
        # convert to unix timestamp
        date = int(date.timestamp())
    return date


def stripe_get_data(
    resource: str,
    start_date: Optional[Any] = None,
    end_date: Optional[Any] = None,
    **kwargs: Any,
) -> Dict[Any, Any]:
    if start_date:
        start_date = transform_date(start_date)
    if end_date:
        end_date = transform_date(end_date)

    if resource == "Subscription":
        kwargs.update({"status": "all"})

    resource_dict = getattr(stripe, resource).list(
        created={"gte": start_date, "lt": end_date}, limit=100, **kwargs
    )
    return dict(resource_dict)
