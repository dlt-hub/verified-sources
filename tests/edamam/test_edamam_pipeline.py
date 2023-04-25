from datetime import datetime
import pytest

import dlt

from pipelines.edamam import nutrition_data

from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts

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
    # assert if table exists
    assert load_table_counts(pipeline, "nutrition_data")['nutrition_data'] == 1
    # assert columns datatype
    table = pipeline.default_schema.data_tables()[0]
    assert table['columns']['ingredient']['data_type'] == "text"
    assert table['columns']['calories']['data_type'] == "bigint"
    assert table['columns']['created_at']['data_type'] == "timestamp"
