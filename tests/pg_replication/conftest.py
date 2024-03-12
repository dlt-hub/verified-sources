import pytest

from typing import Iterator

import dlt


TABLE_NAME = "items"


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
        # drop replication slots
        slot_names = [
            tup[0]
            for tup in c.execute_sql(
                f"SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '_dlt_slot_{src_pl.dataset_name}_%'"
            )
        ]
        for slot_name in slot_names:
            c.execute_sql(f"SELECT pg_drop_replication_slot('{slot_name}');")
        # drop publications
        pub_names = [
            tup[0]
            for tup in c.execute_sql(
                f"SELECT pubname FROM pg_publication WHERE pubname LIKE '_dlt_pub_{src_pl.dataset_name}_%'"
            )
        ]
        for pub_name in pub_names:
            c.execute_sql(f"DROP PUBLICATION IF EXISTS {pub_name};")
