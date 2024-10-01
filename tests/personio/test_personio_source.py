import dlt
import pytest

from sources.personio import personio_source
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.skip("We don't have a Personio test account.")
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline",
        destination=destination_name,
        dataset_name="test_data",
        dev_mode=True,
    )
    # Set per page limit to ensure we use pagination
    load_info = pipeline.run(personio_source())
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts["employees"] >= 31
    assert table_counts["absence_types"] >= 6
    assert table_counts["attendances"] > 0
    assert table_counts["absences"] > 1000


@pytest.mark.skip("We don't have a Personio test account.")
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_incremental_endpoints(destination_name: str) -> None:
    # do the initial load
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline",
        destination=destination_name,
        dataset_name="test_data",
        dev_mode=True,
    )
    info = pipeline.run(personio_source().with_resources("employees"))
    assert_load_info(info)
    info = pipeline.run(personio_source().with_resources("employees"))
    assert_load_info(info, expected_load_packages=0)

    info = pipeline.run(personio_source().with_resources("attendances"))
    assert_load_info(info)
    info = pipeline.run(personio_source().with_resources("attendances"))
    assert_load_info(info, expected_load_packages=0)

    info = pipeline.run(personio_source().with_resources("absences"))
    assert_load_info(info)
    info = pipeline.run(personio_source().with_resources("absences"))
    assert_load_info(info, expected_load_packages=0)
