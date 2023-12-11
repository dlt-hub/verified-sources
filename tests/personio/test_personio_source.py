import dlt
import pytest

from sources.personio import personio_source
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline",
        destination=destination_name,
        dataset_name="test_data",
        full_refresh=True,
    )
    # Set per page limit to ensure we use pagination
    load_info = pipeline.run(personio_source(items_per_page=10))
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    tables = ["employees", "absences", "attendances"]
    assert set(table_counts.keys()) > set(tables)
    assert table_counts["employees"] >= 31
    assert table_counts["absences"] >= 6
    assert table_counts["attendances"] > 0

    # load again to check there are no duplicates
    load_info = pipeline.run(personio_source(items_per_page=10))
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)
    assert set(table_counts.keys()) > set(tables)
    assert table_counts["employees"] >= 31
    assert table_counts["absences"] >= 6
    assert table_counts["attendances"] > 0


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incremental_endpoints(destination_name: str) -> None:
    # do the initial load
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline",
        destination=destination_name,
        dataset_name="test_data",
        full_refresh=True,
    )
    info = pipeline.run(personio_source().with_resources("employees"))
    assert_load_info(info)
    info = pipeline.run(personio_source().with_resources("employees"))
    assert_load_info(info, expected_load_packages=0)

    info = pipeline.run(personio_source().with_resources("attendances"))
    assert_load_info(info)
    info = pipeline.run(personio_source().with_resources("attendances"))
    assert_load_info(info, expected_load_packages=0)
