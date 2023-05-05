import dlt
from stripe_analytics import metrics_resource, incremental_stripe_source, updated_stripe_source


def load_incremental_endpoints(endpoints: list):
    """
    Incremental Endpoint:
    The existing objects of this endpoint do not change over time.
    Therefore, we can download data incrementally.
    This kind of endpoint is a resource with incremental loading based on "append" mode.
    You will load only the newest data without duplicating.

    Make sure you're loading objects that doesn't change over time.
    """
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics",
        destination="duckdb",
        dataset_name="stripe_incremental",
    )
    source = incremental_stripe_source(endpoints=endpoints)
    load_info = pipeline.run(source)
    print(load_info)


def load_updated_endpoints(endpoints: list):
    """
    Updated Endpoint:
    This kind of endpoint is a resource with non-incremental
    loading based on "merge" mode, it means if you want to
    update you data, you will load all your data without duplicating.
    """
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics",
        destination="duckdb",
        dataset_name="stripe_updated",
    )
    source = updated_stripe_source(endpoints=endpoints)
    load_info = pipeline.run(source)
    print(load_info)


def load_data_and_get_metrics():
    """
    With the pipeline you can calculate the most important metrics
    and store them in a database as a resource.
    Store metrics, get calculated metrics from the database, build dashboards.

    Supported metrics:
        Monthly Recurring Revenue (MRR),
        Subscription churn rate.

    Pipeline returns both metrics.

    Use Subscription and Event endpoints to calculate the metrics.
    """

    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics",
        destination="duckdb",
        dataset_name="stripe_metrics",
    )

    #  Event is Incremental endpoint, so we should use 'incremental_stripe_source'.
    source = incremental_stripe_source(endpoints=["Event"])
    load_info = pipeline.run(source)
    print(load_info)

    # Subscription is Updated endpoint, use updated_stripe_source.
    source = updated_stripe_source(endpoints=["Subscription"])
    load_info = pipeline.run(source)
    print(load_info)

    resource = metrics_resource()
    print(list(resource))
    load_info = pipeline.run(resource)
    print(load_info)


if __name__ == "__main__":
    load_data_and_get_metrics()