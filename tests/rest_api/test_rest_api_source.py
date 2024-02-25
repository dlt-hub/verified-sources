from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
import pytest
import dlt

from sources.rest_api import rest_api_source


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_rest_api_source(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api",
        destination=destination_name,
        dataset_name="rest_api_data",
        full_refresh=True,
    )

    config = {
        "client": {
            "base_url": "https://pokeapi.co/api/v2/",
        },
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "limit": 1000,
                },
            }
        },
        "resources": [
            "pokemon",
            "berry",
            "location",
        ],
    }

    load_info = pipeline.run(rest_api_source(config))
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts.keys() == {"pokemon", "berry", "location"}

    assert table_counts["pokemon"] == 1302
    assert table_counts["berry"] == 64
    assert table_counts["location"] == 1036
