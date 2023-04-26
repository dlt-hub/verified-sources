from datetime import datetime
from enum import Enum
from typing import Generator

import dlt
import numpy as np
import pandas as pd
import stripe


class Endpoints(Enum):
    customers: str = "Customer"
    subscriptions: str = "Subscription"
    prices: str = "Price"
    discounts: str = "Coupon"
    events: str = "Event"


def stripe_get_data(resource: Endpoints, start_date=None, end_date=None, **kwargs):
    if start_date:
        # convert to unix timestamp
        start_date = int(start_date.timestamp())
    if end_date:
        # convert to unix timestamp
        end_date = int(end_date.timestamp())

    if resource == Endpoints.subscriptions:
        kwargs.update({"status": "all"})

    resource_list = getattr(stripe, resource.value).list(
        created={"gte": start_date, "lt": end_date}, **kwargs
    )
    return resource_list


@dlt.source
def stripe_source(
    stripe_secret_key: str = dlt.secrets.value,
    limit: int = 100,
    get_all_data: bool = False,
):
    stripe.api_key = stripe_secret_key
    stripe.api_version = "2022-11-15"

    def get_resource(endpoint: Endpoints) -> Generator:
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


def calculate_mrr(df_sub):
    # NORMALIZED PLAN AMOUNT
    df_sub["discount__coupon__percent_off"] = np.where(
        df_sub["discount__coupon__duration"] == "forever",
        df_sub["discount__coupon__percent_off"],
        0,
    )

    df_sub["plan_amount_month"] = np.where(
        df_sub["plan__interval"] == "month",
        (1 / 100)
        * df_sub["plan__amount"]
        * df_sub["quantity"]
        * (1 - df_sub["discount__coupon__percent_off"] / 100),
        np.where(
            df_sub["plan__interval"] == "year",
            (1 / 100)
            * (1 / 12)
            * df_sub["plan__amount"]
            * df_sub["quantity"]
            * (1 - df_sub["discount__coupon__percent_off"] / 100),
            np.nan,
        ),
    )

    df_sub["created"] = pd.to_datetime(df_sub["created"], unit="s").dt.tz_localize(None)

    def total_mrr(df_sub, end_date=datetime(2023, 5, 1)):
        """
        Total MRR
        end_date: first day of the next month
        """
        df_sub = df_sub[df_sub["created"] < end_date]
        return df_sub[df_sub["status"].isin(["active", "past_due"])][
            "plan_amount_month"
        ].sum()

    return total_mrr(df_sub)


def churn_rate(df_event, df_subscription):
    # churned subscribers in the past
    churned_subscriber = len(
        df_event[df_event["type"] == "customer.subscription.deleted"]
    )
    # total active or past_due subscription now
    subscriber = len(df_subscription[df_subscription.status != "canceled"])

    return round(churned_subscriber / (churned_subscriber + subscriber), 3)
