import dlt
import pytest

from sources.personio import personio_source
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="personio",
        destination=destination_name,
        dataset_name="shopify_data",
        full_refresh=True,
    )
    # Set per page limit to ensure we use pagination
    tables = ["employees", "absences"]
    load_info = pipeline.run(personio_source(items_per_page=10).with_resources(*tables))
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    assert set(table_counts.keys()) > set(tables)
    assert table_counts["employees"] >= 31
    assert table_counts["absences"] >= 6

    # load again to check there are no dupicates
    load_info = pipeline.run(personio_source(items_per_page=10).with_resources(*tables))
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    assert set(table_counts.keys()) > set(tables)
    assert table_counts["employees"] >= 31
    assert table_counts["absences"] >= 6
