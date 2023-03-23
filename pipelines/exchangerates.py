import dlt
from exchange_rates import exchangerates_source

CURRENCY_LIST = ["AUD", "BRL", "CAD", "CHF", "CNY", "DKK", "EUR", "GBP"]
BASE_CURRENCY = "EUR"

pipeline = dlt.pipeline(
        pipeline_name="exchangerates", destination="postgres", dataset_name="exchangerates_data"
    )

def get_last_updated_at(default="2023-03-17T00:00:00Z"):
    last_updated_query = (
        f"select max(date)::date from exchangerates_data.exchangerates_resource"
    )
    # Query the database to get the most recent date that the conversion rates were loaded

    with pipeline.sql_client() as client:
        res = client.execute_sql(last_updated_query)
        # todo: error handling - we would need to make sure this only happens for a "table not found" error.
        if not res[0][0]:
            last_val = dlt.current.state().setdefault("date", default)
        else:
            last_val = dlt.current.state().setdefault("date", res[0][0])

    return last_val

def load_euro_conversion_rates_incrementally(currency_list, base_currency):
    """
    Loads the euro conversion rates for the specified currencies incrementally into a PostgreSQL database.

    Args:
        currency_list (list): A list of currency codes to load the conversion rates for.
    """

    # Define a pipeline to load the data into the PostgreSQL database
    last_updated_at = get_last_updated_at()
    load_info = pipeline.run(
        exchangerates_source(currency_list, last_updated_at, base_currency)
    )

    print(load_info)


if __name__ == "__main__":
    # Load the euro conversion rates for the specified currencies
    load_euro_conversion_rates_incrementally(CURRENCY_LIST, BASE_CURRENCY)
