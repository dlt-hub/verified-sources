import dlt
from exchange_rates import exchangerates_source

CURRENCY_LIST = ["AUD", "BRL", "CAD", "CHF", "CNY", "DKK", "EUR", "GBP"]
BASE_CURRENCY = "EUR"


def load_euro_conversion_rates_incrementally(currency_list, base_currency):
    """
    Loads the euro conversion rates for the specified currencies incrementally into a PostgreSQL database.

    Args:
        currency_list (list): A list of currency codes to load the conversion rates for.
    """
    pipeline = dlt.pipeline(
        pipeline_name="exchangerates",
        destination="postgres",
        dataset_name="exchangerates_data",
    )
    # Define a pipeline to load the data into the PostgreSQL database
    load_info = pipeline.run(exchangerates_source(currency_list, base_currency))

    print(load_info)


if __name__ == "__main__":
    # Load the euro conversion rates for the specified currencies
    load_euro_conversion_rates_incrementally(CURRENCY_LIST, BASE_CURRENCY)
