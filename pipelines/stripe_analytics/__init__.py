""" This pipeline uses Stripe API and dlt to load data such as Customer, Subscription, Event and etc. to the database and to calculate the MRR and churn rate. """

from typing import Any, Dict, Generator, Optional

import dlt
import stripe
from dlt.common import pendulum
from dlt.extract.source import DltResource
from pendulum import DateTime

from .helpers import pagination, transform_date
from .metrics import calculate_mrr, churn_rate


@dlt.source
def incremental_stripe_source(
    endpoints: list,
    stripe_secret_key: str = dlt.secrets.value,
    start_date: Optional[DateTime] = None,
    end_date: Optional[DateTime] = None,
) -> Generator[DltResource, Any, None]:
    stripe.api_key = stripe_secret_key
    stripe.api_version = "2022-11-15"
    start_date_unix = transform_date(start_date) if start_date is not None else -1

    def incremental_resource(
        endpoint: str,
        created: Optional[Any] = dlt.sources.incremental(
            "created", initial_value=start_date_unix
        ),
    ) -> Generator[Dict[Any, Any], Any, None]:
        start_value = created.last_value

        yield pagination(endpoint, start_value, end_date)

    for endpoint in endpoints:
        yield dlt.resource(
            incremental_resource,
            name=endpoint,
            write_disposition="append",
            primary_key="id",
        )(endpoint)


@dlt.source
def updated_stripe_source(
    endpoints: list,
    stripe_secret_key: str = dlt.secrets.value,
    start_date: Optional[DateTime] = None,
    end_date: Optional[DateTime] = None,
) -> Generator[DltResource, Any, None]:
    stripe.api_key = stripe_secret_key
    stripe.api_version = "2022-11-15"

    def updated_resource(
        endpoint: str,
    ) -> Generator[Dict[Any, Any], Any, None]:
        yield pagination(endpoint, start_date, end_date)

    for endpoint in endpoints:
        yield dlt.resource(
            updated_resource,
            name=endpoint,
            write_disposition="merge",
            primary_key="id",
        )(endpoint)


@dlt.resource(name="Metrics", write_disposition="append", primary_key="created")
def metrics_resource() -> Generator[Dict[str, Any], Any, None]:
    pipeline = dlt.current.pipeline()  # type: ignore
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM subscription") as table:
            sub_info = table.df()

    # Access to events through the Retrieve Event API is guaranteed only for 30 days.
    # But we probably have old data in the database.
    with pipeline.sql_client() as client:
        with client.execute_query(
            "SELECT * FROM event WHERE created > %s", pendulum.now().subtract(days=30)
        ) as table:
            event_info = table.df()

    mrr = calculate_mrr(sub_info)
    print(f"MRR: {mrr}")

    churn = churn_rate(event_info, sub_info)
    print(f"Churn rate: {round(churn * 100, 1)}%")

    yield {"MRR": mrr, "Churn rate": churn, "created": pendulum.now()}
