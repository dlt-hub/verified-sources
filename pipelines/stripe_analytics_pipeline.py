from datetime import datetime, timedelta
import dlt
from stripe_analytics.stripe_analytics import stripe_source
from stripe_analytics.metrics import calculate_mrr, churn_rate


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="stripe_analytics",
        destination="duckdb",
        dataset_name="stripe_customers_subscriptions",
    )
    source = stripe_source(limit=100, get_all_data=True)

    load_info = pipeline.run(source)
    print(load_info)

    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM subscription") as table:
            sub_info = table.df()

    # Access to events through the Retrieve Event API is guaranteed only for 30 days.
    # But we probably have old data in the database.
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM event WHERE created > %s", datetime.now() - timedelta(30)) as table:
            event_info = table.df()

    mrr = calculate_mrr(sub_info)
    print(f"MRR: {mrr}")

    churn = churn_rate(event_info, sub_info)
    print(f"Churn rate: {churn}")
