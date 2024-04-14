import pytest

from typing import Iterator, Tuple

import dlt
from dlt.common.utils import uniq_id


@pytest.fixture()
def src_config() -> Iterator[Tuple[dlt.Pipeline, str, str]]:
    # random slot and pub to enable parallel runs
    slot = "test_slot_" + uniq_id(4)
    pub = "test_pub" + uniq_id(4)
    # setup
    src_pl = dlt.pipeline(
        pipeline_name="src_pl", destination="postgres", full_refresh=True
    )
    yield src_pl, slot, pub
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
        try:
            c.execute_sql(f"SELECT pg_drop_replication_slot('{slot}');")
        except Exception as e:
            print(e)
        # drop publication
        try:
            c.execute_sql(f"DROP PUBLICATION IF EXISTS {pub};")
        except Exception as e:
            print(e)
