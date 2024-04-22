import pytest
import dlt
from sources.google_ads import google_ads
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts

QUERIES = []
ALL_TABLES = []
QUERY_TABLES = []
DEFAULT_TABLES = []


@pytest.mark.skip(reason="No testing credentials available for the test yet")
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_full_load(destination_name: str) -> None:
    """
    #Checks to see that a full load yields expected results.
    #:param destination_name: Name of the db the data is loaded to.
    #:returns: None
    """
    # load pipeline and check info is loaded correctly and check number of tables is correct
    pipeline = dlt.pipeline(
        pipeline_name="dlt_google_ads_pipeline",
        destination=destination_name,
        full_refresh=False,
        dataset_name="full_load_google_ads",
    )
    load_info = pipeline.run(google_ads())

    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    assert table_counts["customers"] == 1
    assert table_counts["customer_clients"] == 3

    load_info = pipeline.run(google_ads())
    assert load_table_counts(pipeline, *table_names) == table_counts
