import pytest

import dlt

from pipelines.google_play_reviews import google_play_reviews_source

from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts

@pytest.mark.paramtrize("destination_name", ALL_DESTINATIONS)
def test_load_google_play_reviews(destination_name: str) -> None:
    # create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="google_play_reviews",
        destination_name=destination_name,
        dataset_name="google_play_reviews_data",
        full_refresh=True
    )
    info = pipeline.run(google_play_reviews_source(
        package_name="com.facebook.katana"
    ).with_resources("review_and_rating"))
    # test if the job is loaded
    assert_load_info(info)
    # assert if table exists
    assert load_table_counts(
        pipeline, 
        "review_and_rating"
    )["review_and_rating"] == 1
    # assert column datatype
    table = pipeline.default_schema.data_tables()[0]
    assert table["column"]["review_id"]["data_type"] == "text"
    assert table["column"]["comment"]["data_type"] == "text"
    assert table["column"]["rating"]["data_type"] == "double"
    assert table["column"]["device"]["data_type"] == "text"
    assert table["column"]["android_version"]["data_type"] == "text"
    assert table["column"]["app_version"]["data_type"] == "text"
    assert table["column"]["created_at"]["data_type"] == "timestamp"
