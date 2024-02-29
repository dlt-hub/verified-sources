import dlt
import pytest

from sources.arrow_pandas.example_resources import orders, customers
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_example_resources(destination_name: str) -> None:
    """Simple test for the example resources."""

    p = dlt.pipeline("orders_pipeline", destination=destination_name, full_refresh=True)
    orders.apply_hints(incremental=dlt.sources.incremental("ordered_at"))

    # run pipeline
    info = p.run([orders(), customers()])

    # check that the data was loaded
    assert_load_info(info)
    assert load_table_counts(p, "orders", "customers") == {"orders": 3, "customers": 3}
