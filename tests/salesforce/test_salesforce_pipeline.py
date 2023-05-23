from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts
import pytest
import dlt
from pipelines.salesforce import salesforce_source

@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:

    expected_tables = ['account', 'campaign', 'contact', 'lead', 'opportunity', 'pricebook_2', 'pricebook_entry', 'product_2', 'sf_user', 'user_role']

    pipeline = dlt.pipeline(
        pipeline_name="salesforce", destination=destination_name, dataset_name="salesforce_data",
    )
    source = salesforce_source()

    # Add schema hints as needed...
    source.schema.merge_hints({"not_null": ["id"]})

    # Execute the pipeline
    load_info = pipeline.run(source)
    assert_load_info(load_info)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)


    print(set(table_counts.keys()))

    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts['sf_user'] == 7
    assert table_counts['account'] == 13
    assert table_counts['campaign'] == 4
    assert table_counts['contact'] == 20
    assert table_counts['lead'] == 22
    assert table_counts['opportunity'] == 31
    assert table_counts['pricebook_2'] == 2
    assert table_counts['pricebook_entry'] == 34
    assert table_counts['product_2'] == 17
    assert table_counts['user_role'] == 18

    # Execute the pipeline
    source = salesforce_source()

    # Add schema hints as needed...
    source.schema.merge_hints({"not_null": ["id"]})

    # Execute the pipeline
    load_info = pipeline.run(source)

    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert set(table_counts.keys()) >= set(expected_tables)
    assert table_counts['account'] == 13
    assert table_counts['campaign'] == 4
    assert table_counts['contact'] == 20
    assert table_counts['lead'] == 22
    assert table_counts['opportunity'] == 31
    assert table_counts['pricebook_2'] == 2
    assert table_counts['pricebook_entry'] == 34
    assert table_counts['product_2'] == 17
    assert table_counts['sf_user'] == 7
    assert table_counts['user_role'] == 18
