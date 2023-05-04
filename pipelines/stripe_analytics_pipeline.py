import dlt
from stripe_analytics import metrics_resource, stripe_source

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics",
        destination="duckdb",
        dataset_name="stripe_customers_subscriptions",
    )
    source = stripe_source()

    load_info = pipeline.run(source)
    print(load_info)

    resource = metrics_resource(pipeline)
    load_info = pipeline.run(resource)
    print(load_info)
