import pytest
import random
from time import time
from unittest import mock

import dlt
from dlt.common.utils import uniq_id
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.common import pendulum

from dlt.common.pipeline import StateInjectableContext, TSourceState

from pipelines.pipedrive import pipedrive_source, RECENTS_ENTITIES
from pipelines import pipedrive as _pipedrive

from tests.utils import ALL_DESTINATIONS, assert_load_info, assert_query_data


ALL_RESOURCES = {
    "custom_fields_mapping",
    "activities",
    "activityTypes",
    "deals",
    "deals_flow",
    "deals_participants",
    "files",
    "filters",
    "notes",
    "persons",
    "organizations",
    "pipelines",
    "products",
    "stages",
    "users",
}

TESTED_RESOURCES = ALL_RESOURCES - {  # Currently there is no test data for these resources
    "pipelines", "stages", "filters", "files", "activityTypes", "notes"
}


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_all_resources(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination=destination_name, dataset_name='pipedrive_data', full_refresh=True)
    load_info = pipeline.run(pipedrive_source())
    print(load_info)
    assert_load_info(load_info)

    # ALl root tables exist in schema
    assert set(pipeline.default_schema.tables) > TESTED_RESOURCES

    # TODO: validate schema and data: write a test helper for that


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_custom_fields_munger(destination_name: str) -> None:
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination=destination_name, dataset_name='pipedrive_data', full_refresh=True)

    load_info = pipeline.run(pipedrive_source().with_resources('persons', 'products', 'custom_fields_mapping'))

    print(load_info)
    assert_load_info(load_info)

    schema = pipeline.default_schema

    raw_query_string = "SELECT {fields} FROM {table} WHERE {condition} ORDER BY {fields}"

    # test person custom fields data munging
    condition = "name IN ('TEST FIELD 1', 'TEST FIELD 2 ') AND endpoint = 'person'"
    query_string = raw_query_string.format(fields="normalized_name", table="custom_fields_mapping", condition=condition)
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

    # test product custom fields data munging

    condition = "name = 'TEST FIELD 1' AND endpoint='product'"
    query_string = raw_query_string.format(fields="normalized_name", table="custom_fields_mapping", condition=condition)
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

    condition = "endpoint = 'person' AND normalized_name IN ('test_field_1', 'test_field_2')"
    query_string = raw_query_string.format(fields="name", table="custom_fields_mapping", condition=condition)
    table_data = ['TEST FIELD 1', 'TEST FIELD 2 ']
    assert_query_data(pipeline, query_string, table_data)

    condition = "endpoint = 'product' AND normalized_name = 'test_field_1'"
    query_string = raw_query_string.format(fields="name", table="custom_fields_mapping", condition=condition)
    table_data = ['TEST FIELD 1']
    assert_query_data(pipeline, query_string, table_data)


def test_since_timestamp() -> None:
    """since_timestamp is coerced correctly to UTC implicit ISO timestamp and passed to endpoint function"""
    pipeline = dlt.pipeline(pipeline_name='pipedrive')

    with mock.patch('pipelines.pipedrive.recents._get_pages', autospec=True, return_value=iter([])) as m:
        incremental_source = pipedrive_source(since_timestamp='1986-03-03T04:00:00+04:00').with_resources('persons')
        pipeline.extract(incremental_source)

    assert m.call_args.kwargs['extra_params']['since_timestamp'] == '1986-03-03 00:00:00'

    with mock.patch('pipelines.pipedrive.recents._get_pages', autospec=True, return_value=iter([])) as m:
        pipeline.extract(pipedrive_source(since_timestamp=pendulum.parse('1986-03-03T04:00:00+04:00')).with_resources('persons'))  # type: ignore[arg-type]

    assert m.call_args.kwargs['extra_params']['since_timestamp'] == '1986-03-03 00:00:00'

    with mock.patch('pipelines.pipedrive.recents._get_pages', autospec=True, return_value=iter([])) as m:
        no_incremental_source = pipedrive_source(write_disposition='replace').with_resources('persons')
        pipeline.extract(no_incremental_source)

    assert m.call_args.kwargs['extra_params']['since_timestamp'] == '1970-01-01 00:00:00'


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_incremental(destination_name: str) -> None:
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination=destination_name, dataset_name='pipedrive', full_refresh=True)

    # No items older than initial value are loaded
    ts = pendulum.parse('2023-03-15T10:17:44Z')
    source = pipedrive_source(since_timestamp=ts).with_resources('persons', 'custom_fields_mapping')  # type: ignore[arg-type]

    pipeline.run(source)

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT min(update_time) FROM persons") as cur:
            row = cur.fetchone()

    assert row[0] >= ts

    # Just check that incremental state is created
    state: TSourceState = pipeline.state  # type: ignore[assignment]
    assert isinstance(state['sources']['pipedrive']['resources']['persons']['incremental']['update_time|modified'], dict)


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_no_incremental(destination_name: str) -> None:
    pipeline = dlt.pipeline(pipeline_name='pipedrive', destination=destination_name, dataset_name='pipedrive', full_refresh=True)

    # No items older than initial value are loaded
    source = pipedrive_source(write_disposition="replace").with_resources('persons')

    load_info = pipeline.run(source)
    assert_load_info(load_info)

    # Incremental state is not created
    state: TSourceState = pipeline.state  # type: ignore[assignment]
    assert state['sources']['pipedrive'].get('resources', {}).get('persons', {}).get('incremental') is None


def test_resource_settings() -> None:
    source = pipedrive_source()

    resource_names = set(source.resources)

    assert resource_names == ALL_RESOURCES

    assert source.resources['custom_fields_mapping'].write_disposition == 'replace'

    for rs_name in resource_names - {'custom_fields_mapping'}:
        rs = source.resources[rs_name]
        assert rs.write_disposition == 'merge'
        assert rs.table_schema()['columns']['id']['primary_key'] is True
        if rs_name == 'deals_flow':  # Flow has a composite key
            assert rs.table_schema()['columns']['object']['primary_key'] is True


# def test_munger_throughput() -> None:
#     # create pipeline so state is available
#     pipeline = dlt.pipeline()

#     with inject_section(ConfigSectionContext(sections=("sources", __name__, "munger"))):
#         with pipeline._container.injectable_context(StateInjectableContext(state={})) as state:
#             # create N data items with X columns each
#             data_items = []
#             # add 100 renames
#             renames = {uniq_id():{"name": uniq_id()} for _ in range(0, 100)}
#             rename_keys = list(renames.keys())
#             # 10.000 records
#             for _ in range(0, 10000):
#                 # with more or less ~100 columns
#                 d = {uniq_id():uniq_id() for _ in range(0, 95)}
#                 # with ~5 columns to be munged/
#                 for _ in range(0, 5):
#                     assert random.choice(rename_keys) in renames
#                     d[random.choice(rename_keys)] = uniq_id()
#                 # assert len(d) == 100
#                 data_items.append(d)

#             state.state["custom_fields_mapping"] = {"endpoint": renames}

#             start_ts = time()
#             pull_munge_func(data_items, "endpoint")
#             print(f"munging time: {time() - start_ts} seconds")
