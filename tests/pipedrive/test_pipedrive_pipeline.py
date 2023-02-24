import pytest

import dlt

from pipelines.pipedrive import pipedrive_source

from tests.utils import ALL_DESTINATIONS, assert_load_info


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

    load_info = pipeline.run(pipedrive_source().with_resources('persons', 'personFields', 'products', 'productFields'))
    print(load_info)
    assert_load_info(load_info)

    with pipeline.sql_client() as db_client:
        query_string = "SELECT {select_field} FROM {table} WHERE {where_field}=%s"

        with db_client.execute_query(query_string.format(select_field='key', table='person_fields', where_field='name'), 'MULTIPLE OPTIONS ') as cursor:
            rows = list(cursor.fetchone())
            assert rows[0] == 'multiple_options'

        with db_client.execute_query(query_string.format(select_field='key', table='person_fields', where_field='name'), 'CUSTOM NAME I CHOSE') as cursor:
            rows = list(cursor.fetchone())
            assert rows[0] == 'custom_name_i_chose'

        with db_client.execute_query(query_string.format(select_field='key', table='product_fields', where_field='name'), 'SPAM') as cursor:
            rows = list(cursor.fetchone())
            assert rows[0] == 'spam'

        with db_client.execute_query(query_string.format(select_field='spam', table='products', where_field='spam'), 'SS') as cursor:
            rows = list(cursor.fetchone())
            assert rows[0] == 'SS'
