import dlt
from stripe_analytics.stripe_analytics import calculate_mrr, churn_rate, stripe_source

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics",
        destination="duckdb",
        dataset_name="stripe_customers_subscriptions",
    )
    source = stripe_source(limit=100, get_all_data=True)
    print(source.resources.keys())
    print(source.resources.selected.keys())

    load_info = pipeline.run(source)
    print(load_info)

    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM subscription") as table:
            sub_info = table.df()

    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM event") as table:
            event_info = table.df()

    mrr = calculate_mrr(sub_info)
    print(f"MRR: {mrr}")

    churn = churn_rate(event_info, sub_info)
    print(f"Churn rate: {churn}")
