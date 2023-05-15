import pytest

import dlt

from pipelines.firebase import firebase_source

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

    info = pipeline.run(firebase_source().with_resources("realtime_db"))
    # assert if job is loaded
    assert_load_info(info)
    # assert if table exists
    assert load_table_counts(pipeline, "realtime_db")["realtime_db"] == 1
