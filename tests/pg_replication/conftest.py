import pytest

from typing import Iterator

import dlt


@pytest.fixture()
def src_pl() -> Iterator[dlt.Pipeline]:
    # setup
    src_pl = dlt.pipeline(
        pipeline_name="src_pl", destination="postgres", full_refresh=True
    )
    yield src_pl
    # teardown
    with src_pl.sql_client() as c:
        # drop tables
        try:
            c.drop_dataset()
        except Exception as e:
            print(e)
        with c.with_staging_dataset(staging=True):
            try:
                c.drop_dataset()
            except Exception as e:
                print(e)
        # drop replication slot
        c.execute_sql("SELECT pg_drop_replication_slot('test_slot');")
        # drop publication
        c.execute_sql("DROP PUBLICATION IF EXISTS test_pub;")
