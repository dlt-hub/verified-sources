from enum import Enum
from typing import Generator

import dlt
import stripe


class Endpoints(Enum):
    customers: str = "Customer"
    subscriptions: str = "Subscription"
    prices: str = "Price"
    discounts: str = "Coupon"
    events: str = "Event"


def stripe_get_data(resource: Endpoints, start_date=None, end_date=None, **kwargs) -> dict:
    if start_date:
        # convert to unix timestamp
        start_date = int(start_date.timestamp())
    if end_date:
        # convert to unix timestamp
        end_date = int(end_date.timestamp())

    if resource == Endpoints.subscriptions:
        kwargs.update({"status": "all"})

    resource_dict = getattr(stripe, resource.value).list(
        created={"gte": start_date, "lt": end_date}, **kwargs
    )
    return resource_dict


@dlt.source
def stripe_source(
    stripe_secret_key: str = dlt.secrets.value,
    limit: int = 100,
    get_all_data: bool = False,
):

    stripe.api_key = stripe_secret_key
    stripe.api_version = "2022-11-15"

    def get_resource(endpoint: Endpoints) -> Generator[dict]:
        get_more = True
        starting_after = None
        while get_more:
            response = stripe_get_data(
                endpoint, limit=limit, starting_after=starting_after
            )
            get_more = False if not get_all_data else response["has_more"]

            if len(response["data"]) > 0:
                starting_after = response["data"][-1]["id"]

            yield response["data"]

    for endpoint in Endpoints:
        yield dlt.resource(
            get_resource(endpoint),
            name=endpoint.value,
            write_disposition="merge",
            primary_key="id",
        )


