import dlt
from stripe_analytics.stripe_analytics import stripe_source, metrics_resource


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics",
        destination="duckdb",
        dataset_name="stripe_customers_subscriptions",
    )
    source = stripe_source(limit=100, get_all_data=True)

    load_info = pipeline.run(source)
    print(load_info)

    pipeline_metrics = dlt.pipeline(
        pipeline_name="stripe_analytics",
        destination="duckdb",
        dataset_name="stripe_metrics",
    )

    resource = metrics_resource(pipeline)
    load_info = pipeline.run(resource)
    print(load_info)