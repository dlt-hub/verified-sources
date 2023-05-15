from datetime import datetime
import pytest

import dlt

from pipelines.youtube_data import youtube_data

from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts

@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_load_youtube_data(destination_name: str) -> None:
    # create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="youtube_data",
        destination=destination_name,
        dataset_name="youtube_data",
        full_refresh=True
    )

    channel_names = ["mrbeast"]
    start_date="2022-01-01T00:00:00Z"
    end_date="2022-01-02T00:00:00Z"
    max_results = 1    
    info = pipeline.run(youtube_data(channel_names, start_date, end_date, max_results))
    # test if job is loaded
    assert_load_info(info)
    # test if table exists
    assert load_table_counts(pipeline, "youtube_data")["youtube_data"] == 1
    # assert column datatype
    table = pipeline.default_schema.data_tables()[0]
    assert table["columns"]["channel_id"]["data_type"] == "text"
    assert table["columns"]["channel_name"]["data_type"] == "text"
    assert table["columns"]["title"]["data_type"] == "text"
    assert table["columns"]["published"]["data_type"] == "timestamp"
    assert table["columns"]["descriptions"]["data_type"] == "text"
    assert table["columns"]["tags"]["data_type"] == "array"
    assert table["columns"]["tags_count"]["data_type"] == "array"
    assert table["columns"]["views_count"]["data_type"] == "bigint"
    assert table["columns"]["likes_count"]["data_type"] == "bigint"
    assert table["columns"]["dislikes_count"]["data_type"] == "bigint"
    assert table["columns"]["comments_count"]["data_type"] == "bigint"
    assert table["columns"]["created_at"]["data_type"] == "timestamp"
