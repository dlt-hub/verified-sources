import pytest

import dlt

from pipelines.firebase_pipeline import firebase_source_demo, parse_data

from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts

@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_firebase_source(destination: str) -> None:
    # create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="firebase_database",
        destination_name=destination_name,
        dataset_name="firebase",
        full_refresh=True
    )

    info = pipeline.run(firebase_source_demo().with_resources("realtime_db").add_map(parse_data))
    # assert if job is loaded
    assert_load_info(info)
    # assert if table exists
    assert load_table_counts(pipeline, "realtime_db")["realtime_db"] == 1
    # assert columns datatype
    table = pipeline.default_schema.data_tables()[0]
    assert table["columns"]["band_name"]["data_type"] == "text"
    assert table["columns"]["album_name"]["data_type"] == "text"
    assert table["columns"]["year"]["data_type"] == "bigint"
