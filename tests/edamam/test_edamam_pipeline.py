from datetime import datetime
import pytest

import dlt

from pipelines.edamam import edamam_source, nutrition_data

from tests.utils import ALL_DESTINATIONS, assert_load_info

@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_load_nutrition_data(destination_name: str) -> None:
    # create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="edamam",
        destination=destination_name,
        dataset_name="edamam_data",
        full_refresh=True
    )
    ingredients = ['100 gr chicken']
    info = pipeline.run(nutrition_data(ingredients))
    # test if jobs is loaded
    assert_load_info(info)
    # assert the schema
    table = pipeline.default_schema.data_tables()
    assert len(table) == 1
    # assert that table is a dict
    assert isinstance(table[0], dict)
    # assert the columns datatype
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM nutrition_data LIMIT 1") as query:
            rows = list(query.fetchall())
            # assert that we return something
            assert len(rows) != 0
            # assert that ingredient column is a string
            assert isinstance(rows[0][0], str)
            # assert that calorie column is an int
            assert isinstance(rows[0][1], int)
            # assert that created_at column is a time format
            assert isinstance(rows[0][2], datetime)
