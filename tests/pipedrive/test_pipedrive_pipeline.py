import pytest
import random
from time import time

import dlt
from dlt.common.utils import uniq_id
from dlt.pipeline.state import StateInjectableContext

from pipelines.pipedrive import pipedrive_source
from pipelines.pipedrive.custom_fields_munger import pull_munge_func

from tests.utils import ALL_DESTINATIONS, assert_load_info, assert_query_data


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination=destination_name, dataset_name='pipedrive_data', full_refresh=True)
    load_info = pipeline.run(pipedrive_source())
    print(load_info)
    assert_load_info(load_info)
    # TODO: validate schema and data: write a test helper for that


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_custom_fields_munger(destination_name: str) -> None:

    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination=destination_name, dataset_name='pipedrive_data', full_refresh=True)

    load_info = pipeline.run(pipedrive_source().with_resources('persons', 'personFields', 'products', 'productFields', 'custom_fields_mapping'))
    print(load_info)
    assert_load_info(load_info)

    schema = pipeline.default_schema

    raw_query_string = "SELECT {fields} FROM {table} WHERE {condition} ORDER BY {fields}"

    # test personFields' custom fields data munging

    condition = "name IN ('TEST FIELD 1', 'TEST FIELD 2 ')"
    query_string = raw_query_string.format(fields="key", table="person_fields", condition=condition)
    table_data = ['test_field_1', 'test_field_2']
    assert_query_data(pipeline, query_string, table_data)

    # test persons' custom fields data munging

    persons_table = schema.get_table('persons')
    assert 'test_field_1' in persons_table['columns']
    assert 'test_field_2' in persons_table['columns']

    condition = "test_field_1 = 'Test Value 1'"
    query_string = raw_query_string.format(fields="test_field_1", table="persons", condition=condition)
    table_data = ['Test Value 1']
    assert_query_data(pipeline, query_string, table_data)

    condition = "test_field_2 = 'Test Value 2'"
    query_string = raw_query_string.format(fields="test_field_2", table="persons", condition=condition)
    table_data = ['Test Value 2']
    assert_query_data(pipeline, query_string, table_data)

    # test productFields' custom fields data munging

    condition = "name = 'TEST FIELD 1'"
    query_string = raw_query_string.format(fields="key", table="product_fields", condition=condition)
    table_data = ['test_field_1']
    assert_query_data(pipeline, query_string, table_data)

    # test products' custom fields data munging

    products_table = schema.get_table('products')
    assert 'test_field_1' in products_table['columns']

    condition = "test_field_1 = 'Test Value 1'"
    query_string = raw_query_string.format(fields="test_field_1", table="products", condition=condition)
    table_data = ['Test Value 1']
    assert_query_data(pipeline, query_string, table_data)

    # test custom fields mapping

    custom_fields_mapping = schema.get_table('custom_fields_mapping')
    assert 'endpoint' in custom_fields_mapping['columns']
    assert 'hash_string' in custom_fields_mapping['columns']
    assert 'name' in custom_fields_mapping['columns']
    assert 'normalized_name' in custom_fields_mapping['columns']

    condition = "endpoint = 'personFields' AND normalized_name IN ('test_field_1', 'test_field_2')"
    query_string = raw_query_string.format(fields="name", table="custom_fields_mapping", condition=condition)
    table_data = ['TEST FIELD 1', 'TEST FIELD 2 ']
    assert_query_data(pipeline, query_string, table_data)

    condition = "endpoint = 'productFields' AND normalized_name = 'test_field_1'"
    query_string = raw_query_string.format(fields="name", table="custom_fields_mapping", condition=condition)
    table_data = ['TEST FIELD 1']
    assert_query_data(pipeline, query_string, table_data)


def test_munger_throughput() -> None:
    # create pipeline so state is available
    pipeline = dlt.pipeline()

    with pipeline._container.injectable_context(StateInjectableContext(state={})):
        # create N data items with X columns each
        data_items = []
        # add 100 renames
        renames = {uniq_id():{"name": uniq_id()} for _ in range(0, 100)}
        rename_keys = list(renames.keys())
        # 10.000 records
        for _ in range(0, 10000):
            # with more or less ~100 columns
            d = {uniq_id():uniq_id() for _ in range(0, 95)}
            # with ~5 columns to be munged/
            for _ in range(0, 5):
                assert random.choice(rename_keys) in renames
                d[random.choice(rename_keys)] = uniq_id()
            # assert len(d) == 100
            data_items.append(d)

        state = dlt.state()
        state["custom_fields_mapping"] = {"endpoint": renames}

        start_ts = time()
        pull_munge_func(data_items, "endpoint")
        print(f"munging time: {time() - start_ts} seconds")
