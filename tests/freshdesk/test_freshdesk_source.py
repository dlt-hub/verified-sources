import dlt
import pytest
from utils import assert_load_info, load_table_counts

from sources.freshdesk import freshdesk_source, tickets


@pytest.mark.parametrize("destination_name", ["bigquery"])
def test_load_all_endpoints(destination_name: str) -> None:
    # mind the full_refresh flag - it makes sure that data is loaded to unique dataset. this allows you to run the tests on the same database in parallel
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_name",
        destination=destination_name,
        dataset_name="test_dataset_name",
        full_refresh=True,
    )
    endpoints = ["agents", "companies", "contacts", "groups", "roles"]
    all_endpoints = freshdesk_source(endpoints=endpoints)
    # do not load child tables
    all_endpoints.max_table_nesting = 0
    info = pipeline.run(all_endpoints)
    # make sure all jobs were loaded
    assert_load_info(info)
    assert load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()]
    ) == {"agents": 1, "companies": 1, "contacts": 15, "groups": 3, "roles": 8}


@pytest.mark.parametrize("destination_name", ["bigquery"])
def test_incremental_tickets_load(destination_name: str) -> None:
    # do the initial load
    pipeline = dlt.pipeline(
        pipeline_name="freshdesk_tickets",
        destination=destination_name,
        dataset_name="freshdesk_tickets",
        full_refresh=True,
    )
    info = pipeline.run(tickets(created_at="2022-01-01T00:00:00Z"))
    assert_load_info(info)

    def get_tickets_count() -> int:
        with pipeline.sql_client() as c:
            with c.execute_query("SELECT COUNT(1) FROM tickets") as cur:
                rows = list(cur.fetchall())
                assert len(rows) == 1
                return rows[0][0]

    ticket_no = get_tickets_count()
    assert ticket_no > 0  # should have tickets

    # do load with the same range into the existing dataset
    data = tickets(created_at="2022-01-01T00:00:00Z")
    info = pipeline.run(data)
    # the dlt figured out that there's no new data at all and skipped the loading package
    assert_load_info(info, expected_load_packages=0)
    # there are no more tickets as pipeline is skipping existing tickets
    assert get_tickets_count() == ticket_no
