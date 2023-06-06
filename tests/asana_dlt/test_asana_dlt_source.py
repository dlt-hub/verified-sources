from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
import pytest
import dlt
from sources.asana_dlt import asana_source


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="asana",
        destination=destination_name,
        dataset_name="asana_data",
        full_refresh=True,
    )
    load_info = pipeline.run(asana_source())
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    # for now only check main tables
    expected_tables = [
        "projects",
        "tasks",
        "sections",
        "workspaces",
        "tags",
        "stories",
        "teams",
        "users",
    ]
    assert set(table_counts.keys()) > set(expected_tables)

    print(table_counts)

    assert table_counts["workspaces"] == 1
    assert table_counts["tags"] == 2
    assert table_counts["projects"] == 2
    assert table_counts["sections"] == 6
    assert table_counts["tasks"] == 6
    assert table_counts["stories"] == 13
    assert table_counts["teams"] == 1
    assert table_counts["users"] == 4

    # load tasks incrementally
    load_info = pipeline.run(asana_source().with_resources("tasks"))
    # no data loaded
    assert_load_info(load_info, 0)
