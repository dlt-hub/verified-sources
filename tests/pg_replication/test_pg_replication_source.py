import pytest

import glob
import os
import threading
from contextlib import closing
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from copy import deepcopy
import psycopg2
import sqlalchemy

import dlt
from dlt.common import Decimal
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.schema.utils import get_dedup_sort_tuple
from dlt.common.utils import uniq_id
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_table

from tests.utils import (
    ALL_DESTINATIONS,
    TEST_STORAGE_ROOT,
    assert_load_info,
    load_table_counts,
    get_table_metrics,
)
from sources.pg_replication import (
    helpers,
    init_replication,
    replication_resource,
    replication_slot_exists,
)
from sources.pg_replication.helpers import get_pg_version
from sources.pg_replication.exceptions import (
    ColumnNameCollisionException,
    IncompatibleColumnTypeException,
    IncompatiblePostgresVersionException,
    NoPrimaryKeyException,
    PublicationPrivilegesException,
    TableNameCollisionException,
    ValueConversionException,
)
from sources.pg_replication.schema_types import TReflectionLevel

from .cases import (
    PG_TYPES_DDL,
    PG_TYPES_ROW,
    TABLE_ROW_ALL_DATA_TYPES,
    TABLE_UPDATE_COLUMNS_SCHEMA,
)
from .utils import (
    add_pk,
    assert_loaded_data,
    create_tables,
    is_super_user,
    run_sql,
    table_rows,
)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_schema_replication(
    src_config: Tuple[dlt.Pipeline, str, str], destination_name: str
) -> None:
    @dlt.resource(write_disposition="merge", primary_key="id_x")
    def tbl_x(data):
        yield data

    @dlt.resource(write_disposition="merge", primary_key="id_y")
    def tbl_y(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    src_pl.run(
        [
            tbl_x({"id_x": 1, "val_x": "foo"}),
            tbl_y({"id_y": 1, "val_y": True}),
        ]
    )
    add_pk(src_pl.sql_client, "tbl_x", "id_x")
    add_pk(src_pl.sql_client, "tbl_y", "id_y")

    snapshots = init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        table_names=[
            "tbl_x",
            "tbl_y",
        ],  # will initialize schema replication and get list of snapshots from sql_database, set to None if creds have superuser privileges
        schema_name=src_pl.dataset_name,
        persist_snapshots=True,
    )

    changes = replication_resource(slot_name, pub_name)

    src_pl.run(
        [
            tbl_x([{"id_x": 2, "val_x": "bar"}, {"id_x": 3, "val_x": "baz"}]),
            tbl_y({"id_y": 2, "val_y": False}),
        ]
    )

    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, dev_mode=True
    )

    info = dest_pl.run(snapshots)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y") == {"tbl_x": 1, "tbl_y": 1}
    exp_tbl_x = [{"id_x": 1, "val_x": "foo"}]
    exp_tbl_y = [{"id_y": 1, "val_y": True}]
    assert_loaded_data(dest_pl, "tbl_x", ["id_x", "val_x"], exp_tbl_x, "id_x")
    assert_loaded_data(dest_pl, "tbl_y", ["id_y", "val_y"], exp_tbl_y, "id_y")

    # process changes
    print("process changes")
    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y") == {"tbl_x": 3, "tbl_y": 2}
    exp_tbl_x = [
        {"id_x": 1, "val_x": "foo"},
        {"id_x": 2, "val_x": "bar"},
        {"id_x": 3, "val_x": "baz"},
    ]
    exp_tbl_y = [{"id_y": 1, "val_y": True}, {"id_y": 2, "val_y": False}]
    assert_loaded_data(dest_pl, "tbl_x", ["id_x", "val_x"], exp_tbl_x, "id_x")
    assert_loaded_data(dest_pl, "tbl_y", ["id_y", "val_y"], exp_tbl_y, "id_y")

    # add new table and load data, we should receive it in publication slot
    # after adding it to the publication
    @dlt.resource(write_disposition="append", primary_key="id_z")
    def tbl_z(data):
        yield data

    # create the table and add pk first
    src_pl.run(tbl_z([{"id_z": 1, "val_x": "initial"}]))
    add_pk(src_pl.sql_client, "tbl_z", "id_z")

    # add the new table to the existing publication before inserting data we want to capture
    init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        table_names=["tbl_x", "tbl_y", "tbl_z"],
        schema_name=src_pl.dataset_name,
    )

    # now insert the data that should be captured in replication
    src_pl.run(tbl_z([{"id_z": 2, "val_x": "bar"}, {"id_z": 3}]))
    src_pl.run(tbl_z([{"id_z": 4, "val_x": "bar"}, {"id_z": 5}]))

    info = dest_pl.run(replication_resource(slot_name, pub_name))
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y", "tbl_z") == {
        "tbl_x": 3,
        "tbl_y": 2,
        "tbl_z": 4,
    }
    # make sure PK was added
    assert (
        dest_pl.default_schema.tables["tbl_x"]["columns"]["id_x"]["primary_key"] is True
    )
    assert (
        dest_pl.default_schema.tables["tbl_z"]["columns"]["id_z"]["primary_key"] is True
    )


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_core_functionality(
    src_config: Tuple[dlt.Pipeline, str, str], destination_name: str, backend: str
) -> None:
    @dlt.resource(write_disposition="merge", primary_key="id_x")
    def tbl_x(data):
        yield data

    @dlt.resource(write_disposition="merge", primary_key="id_y")
    def tbl_y(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    src_pl.run(
        [
            tbl_x({"id_x": 1, "val_x": "foo"}),
            tbl_y({"id_y": 1, "val_y": True}),
        ]
    )
    add_pk(src_pl.sql_client, "tbl_x", "id_x")
    add_pk(src_pl.sql_client, "tbl_y", "id_y")

    snapshots = init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y"),
        persist_snapshots=True,
        backend=backend,
    )

    changes = replication_resource(slot_name, pub_name, backend=backend)

    src_pl.run(
        [
            tbl_x([{"id_x": 2, "val_x": "bar"}, {"id_x": 3, "val_x": "baz"}]),
            tbl_y({"id_y": 2, "val_y": False}),
        ]
    )

    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, dev_mode=True
    )

    # initial load
    info = dest_pl.run(snapshots)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y") == {"tbl_x": 1, "tbl_y": 1}
    exp_tbl_x = [{"id_x": 1, "val_x": "foo"}]
    exp_tbl_y = [{"id_y": 1, "val_y": True}]
    assert_loaded_data(dest_pl, "tbl_x", ["id_x", "val_x"], exp_tbl_x, "id_x")
    assert_loaded_data(dest_pl, "tbl_y", ["id_y", "val_y"], exp_tbl_y, "id_y")

    # process changes
    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y") == {"tbl_x": 3, "tbl_y": 2}
    exp_tbl_x = [
        {"id_x": 1, "val_x": "foo"},
        {"id_x": 2, "val_x": "bar"},
        {"id_x": 3, "val_x": "baz"},
    ]
    exp_tbl_y = [{"id_y": 1, "val_y": True}, {"id_y": 2, "val_y": False}]
    assert_loaded_data(dest_pl, "tbl_x", ["id_x", "val_x"], exp_tbl_x, "id_x")
    assert_loaded_data(dest_pl, "tbl_y", ["id_y", "val_y"], exp_tbl_y, "id_y")

    # change single table
    src_pl.run(tbl_y({"id_y": 3, "val_y": True}))

    # process changes
    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y") == {"tbl_x": 3, "tbl_y": 3}
    exp_tbl_y = [
        {"id_y": 1, "val_y": True},
        {"id_y": 2, "val_y": False},
        {"id_y": 3, "val_y": True},
    ]
    assert_loaded_data(dest_pl, "tbl_x", ["id_x", "val_x"], exp_tbl_x, "id_x")
    assert_loaded_data(dest_pl, "tbl_y", ["id_y", "val_y"], exp_tbl_y, "id_y")

    # update tables
    with src_pl.sql_client() as c:
        qual_name = src_pl.sql_client().make_qualified_table_name("tbl_x")
        c.execute_sql(f"UPDATE {qual_name} SET val_x = 'foo_updated' WHERE id_x = 1;")
        qual_name = src_pl.sql_client().make_qualified_table_name("tbl_y")
        c.execute_sql(f"UPDATE {qual_name} SET val_y = false WHERE id_y = 1;")

    # process changes
    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y") == {"tbl_x": 3, "tbl_y": 3}
    exp_tbl_x = [
        {"id_x": 1, "val_x": "foo_updated"},
        {"id_x": 2, "val_x": "bar"},
        {"id_x": 3, "val_x": "baz"},
    ]
    exp_tbl_y = [
        {"id_y": 1, "val_y": False},
        {"id_y": 2, "val_y": False},
        {"id_y": 3, "val_y": True},
    ]
    assert_loaded_data(dest_pl, "tbl_x", ["id_x", "val_x"], exp_tbl_x, "id_x")
    assert_loaded_data(dest_pl, "tbl_y", ["id_y", "val_y"], exp_tbl_y, "id_y")

    # delete from table
    with src_pl.sql_client() as c:
        qual_name = src_pl.sql_client().make_qualified_table_name("tbl_x")
        c.execute_sql(f"DELETE FROM {qual_name} WHERE id_x = 1;")

    # process changes
    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y") == {"tbl_x": 2, "tbl_y": 3}
    exp_tbl_x = [{"id_x": 2, "val_x": "bar"}, {"id_x": 3, "val_x": "baz"}]
    exp_tbl_y = [
        {"id_y": 1, "val_y": False},
        {"id_y": 2, "val_y": False},
        {"id_y": 3, "val_y": True},
    ]
    assert_loaded_data(dest_pl, "tbl_x", ["id_x", "val_x"], exp_tbl_x, "id_x")
    assert_loaded_data(dest_pl, "tbl_y", ["id_y", "val_y"], exp_tbl_y, "id_y")


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_without_init_load(
    src_config: Tuple[dlt.Pipeline, str, str], destination_name: str
) -> None:
    @dlt.resource(write_disposition="merge", primary_key="id_x")
    def tbl_x(data):
        yield data

    @dlt.resource(write_disposition="merge", primary_key="id_y")
    def tbl_y(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    # create postgres table
    # since we're skipping initial load, these records should not be in the replicated table
    src_pl.run(
        [
            tbl_x({"id_x": 1, "val_x": "foo"}),
            tbl_y({"id_y": 1, "val_y": True}),
        ]
    )
    add_pk(src_pl.sql_client, "tbl_x", "id_x")
    add_pk(src_pl.sql_client, "tbl_y", "id_y")

    # initialize replication and create resource for changes
    init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y"),
    )
    changes = replication_resource(slot_name, pub_name)

    # change postgres table after replication has been initialized
    # these records should be in the replicated table
    src_pl.run(
        [
            tbl_x([{"id_x": 2, "val_x": "bar"}, {"id_x": 3, "val_x": "baz"}]),
            tbl_y({"id_y": 2, "val_y": False}),
        ]
    )

    # load changes to destination and assert expectations
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, dev_mode=True
    )
    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y") == {"tbl_x": 2, "tbl_y": 1}
    exp_tbl_x = [{"id_x": 2, "val_x": "bar"}, {"id_x": 3, "val_x": "baz"}]
    exp_tbl_y = [{"id_y": 2, "val_y": False}]
    assert_loaded_data(dest_pl, "tbl_x", ["id_x", "val_x"], exp_tbl_x, "id_x")
    assert_loaded_data(dest_pl, "tbl_y", ["id_y", "val_y"], exp_tbl_y, "id_y")

    # delete from table
    with src_pl.sql_client() as c:
        qual_name = src_pl.sql_client().make_qualified_table_name("tbl_x")
        c.execute_sql(f"DELETE FROM {qual_name} WHERE id_x = 2;")

    # process change and assert expectations
    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y") == {"tbl_x": 1, "tbl_y": 1}
    exp_tbl_x = [{"id_x": 3, "val_x": "baz"}]
    exp_tbl_y = [{"id_y": 2, "val_y": False}]
    assert_loaded_data(dest_pl, "tbl_x", ["id_x", "val_x"], exp_tbl_x, "id_x")
    assert_loaded_data(dest_pl, "tbl_y", ["id_y", "val_y"], exp_tbl_y, "id_y")


def test_insert_only(src_config: Tuple[dlt.Pipeline, str, str]) -> None:
    def items(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    # create postgres table with single record
    src_pl.run(items({"id": 1, "foo": "bar"}))

    # initialize replication and create resource for changes
    init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="items",
        publish="insert",
    )
    changes = replication_resource(slot_name, pub_name)

    # insert a record in postgres table
    src_pl.run(items({"id": 2, "foo": "bar"}))

    # extract items from resource
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", dev_mode=True)
    extract_info = dest_pl.extract(changes)
    assert get_table_metrics(extract_info, "items")["items_count"] == 1

    # do an update and a delete—these operations should not lead to items in the resource
    with src_pl.sql_client() as c:
        qual_name = src_pl.sql_client().make_qualified_table_name("items")
        c.execute_sql(f"UPDATE {qual_name} SET foo = 'baz' WHERE id = 2;")
        c.execute_sql(f"DELETE FROM {qual_name} WHERE id = 2;")
    extract_info = dest_pl.extract(changes)
    assert (
        get_table_metrics(extract_info, "items") is None
    )  # there should be no metrics for the "items" table


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("give_hints", [True, False])
@pytest.mark.parametrize("init_load", [True, False])
def test_mapped_data_types(
    src_config: Tuple[dlt.Pipeline, str, str],
    destination_name: str,
    give_hints: bool,
    init_load: bool,
) -> None:
    """Assert common data types (the ones mapped in PostgresTypeMapper) are properly handled."""

    data = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
    column_schema = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)

    # resource to load data into postgres source table
    @dlt.resource(primary_key="col1", write_disposition="merge", columns=column_schema)
    def items(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    # create postgres table with single record containing all data types
    src_pl.run(items(data))
    add_pk(src_pl.sql_client, "items", "col1")

    # initialize replication and create resources
    snapshot = init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="items",
        persist_snapshots=init_load,
        columns={"items": column_schema} if give_hints else None,
    )

    changes = replication_resource(
        slot_name=slot_name,
        pub_name=pub_name,
        columns={"items": column_schema} if give_hints else None,
    )

    # initial load
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, dev_mode=True
    )
    if init_load:
        info = dest_pl.run(snapshot)
        assert_load_info(info)
        assert load_table_counts(dest_pl, "items")["items"] == 1

    # insert two records in postgres table
    r1 = deepcopy(data)
    r2 = deepcopy(data)
    r1["col1"] = 1
    r2["col1"] = 2
    src_pl.run(items([r1, r2]))

    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "items")["items"] == 3 if init_load else 2

    if give_hints:
        # compare observed with expected column types
        observed = dest_pl.default_schema.get_table("items")["columns"]
        for name, expected in column_schema.items():
            assert observed[name]["data_type"] == expected["data_type"]
            # postgres bytea does not have precision
            if (
                expected.get("precision") is not None
                and expected["data_type"] != "binary"
            ):
                assert observed[name]["precision"] == expected["precision"]

    # update two records in postgres table
    # this does two deletes and two inserts because dlt implements "merge" as "delete-and-insert"
    # as such, postgres will create four replication messages: two of type Delete and two of type Insert
    r1["col2"] = 1.5
    r2["col3"] = False
    src_pl.run(items([r1, r2]))

    # process changes and assert expectations
    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "items")["items"] == 3 if init_load else 2
    exp = [
        {"col1": 1, "col2": 1.5, "col3": True},
        {"col1": 2, "col2": 898912.821982, "col3": False},
        {
            "col1": 989127831,
            "col2": 898912.821982,
            "col3": True,
        },  # only present with init load
    ]
    if not init_load:
        del exp[-1]
    assert_loaded_data(dest_pl, "items", ["col1", "col2", "col3"], exp, "col1")

    # now do an actual update, so postgres will create a replication message of type Update
    with src_pl.sql_client() as c:
        qual_name = src_pl.sql_client().make_qualified_table_name("items")
        c.execute_sql(f"UPDATE {qual_name} SET col2 = 2.5 WHERE col1 = 2;")

    # process change and assert expectation
    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "items")["items"] == 3 if init_load else 2
    exp = [{"col1": 2, "col2": 2.5, "col3": False}]
    assert_loaded_data(
        dest_pl, "items", ["col1", "col2", "col3"], exp, "col1", "col1 = 2"
    )


@pytest.mark.parametrize("init_load", [True, False])
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_timestamp_without_tz_and_json(
    src_config: Tuple[dlt.Pipeline, str, str],
    init_load: bool,
    destination_name: str,
) -> None:
    """Test that PostgreSQL OID 114 (json) and OID 1114 (timestamp without timezone) are correctly mapped.

    This test verifies the fix for the issue where pg_replication source didn't handle these
    data types correctly, causing them to default to 'text' type instead of proper type mapping.
    """
    data = deepcopy({k: TABLE_ROW_ALL_DATA_TYPES[k] for k in ("col4", "col9")})
    data["col0"] = deepcopy(data["col4"])  # same value as col4

    # resource to load data into postgres source table
    @dlt.resource(columns={"col9": {"data_type": "json"}})
    def items(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    # create postgres table with the three records
    src_pl.run(items(data))

    # ensure source has expected col types for testing:
    # col0: timestamp without time zone (OID 1114)
    # col4: timestamp with time zone (OID 1184)
    # col9: jsonb (OID 3802)
    with src_pl.sql_client() as c:
        query = f"""
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = '{src_pl.dataset_name}'
            AND table_name = 'items'
            AND column_name = %s;
        """
        col0_type = c.execute_sql(query, ("col0"))[0][0]
        assert "timestamp with time zone" == col0_type
        # we need to alter type to timestamp without time zone for col0
        qual_name = src_pl.sql_client().make_qualified_table_name("items")
        c.execute_sql(
            f"ALTER TABLE {qual_name} ALTER COLUMN col0 TYPE timestamp WITHOUT TIME ZONE USING (col0 AT TIME ZONE 'UTC');"
        )
        col0_type = c.execute_sql(query, ("col0"))[0][0]
        assert "timestamp without time zone" == col0_type

        col4_type = c.execute_sql(query, ("col4"))[0][0]
        assert "timestamp with time zone" == col4_type

        # since dlt's PostgresTypeMapper always loads json as jsonb,
        # we need to alter type to test json
        c.execute_sql(
            f"ALTER TABLE {qual_name} ALTER COLUMN col9 TYPE json USING col9::json;"
        )
        col9_type = c.execute_sql(query, ("col9"))[0][0]
        assert "json" == col9_type

    # load to destination
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl",
        destination=destination_name,
    )

    # create snapshot resource based on init_load parameter
    snapshot = init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="items",
        publish="insert",
        persist_snapshots=init_load,
    )
    changes = replication_resource(slot_name, pub_name)

    if init_load:
        info = dest_pl.run(snapshot)
        assert_load_info(info)

    src_pl.run(items(data))
    info = dest_pl.run(changes)
    assert_load_info(info)

    col_schemas = dest_pl.default_schema.get_table_columns("items")
    assert "timestamp" == col_schemas["col0"]["data_type"]
    assert not col_schemas["col0"].get("timezone")
    assert "timestamp" == col_schemas["col4"]["data_type"]
    assert col_schemas["col4"].get("timezone") is True
    assert "json" == col_schemas["col9"]["data_type"]


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_unmapped_data_types(
    src_config: Tuple[dlt.Pipeline, str, str], destination_name: str
) -> None:
    """Assert postgres data types that aren't explicitly mapped default to "text" type."""
    src_pl, slot_name, pub_name = src_config

    # create postgres table with some unmapped types
    with src_pl.sql_client() as c:
        c.create_dataset()
        c.execute_sql(
            "CREATE TABLE data_types (bit_col bit(1), box_col box, uuid_col uuid);"
        )

    # initialize replication and create resource
    init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="data_types",
        publish="insert",
    )
    changes = replication_resource(slot_name, pub_name)

    # insert record in source table to create replication item
    with src_pl.sql_client() as c:
        c.execute_sql(
            "INSERT INTO data_types VALUES"
            " (B'1', box '((1,1), (0,0))', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');"
        )

    # run destination pipeline and assert resulting data types
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, dev_mode=True
    )
    dest_pl.extract(changes)
    dest_pl.normalize()
    columns = dest_pl.default_schema.get_table_columns("data_types")
    assert columns["bit_col"]["data_type"] == "text"
    assert columns["box_col"]["data_type"] == "text"
    assert columns["uuid_col"]["data_type"] == "text"


@pytest.mark.parametrize("publish", ["insert", "insert, update, delete"])
def test_write_disposition(
    src_config: Tuple[dlt.Pipeline, str, str], publish: str
) -> None:
    @dlt.resource
    def items(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    # create postgres table
    src_pl.run(items({"id": 1, "val": True}))
    add_pk(src_pl.sql_client, "items", "id")

    # create resources
    snapshot = init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="items",
        publish=publish,
        persist_snapshots=True,
    )

    # assert write disposition on snapshot resource
    expected_write_disposition = "append" if publish == "insert" else "merge"
    assert snapshot.write_disposition == expected_write_disposition
    if publish == "insert":
        # on the existing slot the appended initial load would repeat the replicated inserts
        with pytest.raises(ValueError, match="loaded twice"):
            init_replication(
                slot_name,
                pub_name,
                src_pl.dataset_name,
                "items",
                initial_snapshot="direct",
            )

    # assert write disposition on tables dispatched by changes resource
    changes = replication_resource(slot_name, pub_name)
    src_pl.run(items({"id": 2, "val": True}))
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", dev_mode=True)
    dest_pl.extract(changes)
    assert (
        dest_pl.default_schema.get_table("items")["write_disposition"]
        == expected_write_disposition
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("init_load", [True, False])
@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_include_columns(
    src_config: Tuple[dlt.Pipeline, str, str],
    destination_name: str,
    init_load: bool,
    backend: str,
) -> None:
    def get_cols(pipeline: dlt.Pipeline, table_name: str) -> Set[str]:
        columns = pipeline.dataset()[table_name].columns
        return {name for name in columns if not name.startswith("_dlt_")}

    @dlt.resource
    def tbl_x(data):
        yield data

    @dlt.resource
    def tbl_y(data):
        yield data

    @dlt.resource
    def tbl_z(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    # create three postgres tables
    src_pl.run(
        [
            tbl_x({"id_x": 1, "val_x": "foo", "another_col_x": 1}),
            tbl_y({"id_y": 1, "val_y": "foo", "another_col_y": 1}),
            tbl_z({"id_z": 1, "val_z": "foo", "another_col_z": 1}),
        ]
    )

    # initialize replication and create resources
    include_columns = {
        "tbl_x": ["id_x", "val_x"],
        "tbl_y": ["id_y", "val_y"],
        # tbl_z is not specified, hence all columns should be included
    }
    snapshots = init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y", "tbl_z"),
        publish="insert",
        persist_snapshots=init_load,
        include_columns=include_columns,
        backend=backend,
    )
    changes = replication_resource(
        slot_name=slot_name,
        pub_name=pub_name,
        include_columns=include_columns,
        backend=backend,
    )

    # update three postgres tables
    src_pl.run(
        [
            tbl_x({"id_x": 2, "val_x": "foo", "another_col_x": 1}),
            tbl_y({"id_y": 2, "val_y": "foo", "another_col_y": 1}),
            tbl_z({"id_z": 2, "val_z": "foo", "another_col_z": 1}),
        ]
    )

    # load to destination and assert column expectations
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, dev_mode=True
    )
    if init_load:
        dest_pl.run(snapshots)
        assert get_cols(dest_pl, "tbl_x") == {"id_x", "val_x"}
        assert get_cols(dest_pl, "tbl_y") == {"id_y", "val_y"}
        assert get_cols(dest_pl, "tbl_z") == {"id_z", "val_z", "another_col_z"}
    dest_pl.run(changes)
    assert get_cols(dest_pl, "tbl_x") == {"id_x", "val_x"}
    assert get_cols(dest_pl, "tbl_y") == {"id_y", "val_y"}
    assert get_cols(dest_pl, "tbl_z") == {"id_z", "val_z", "another_col_z"}


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("init_load", [True, False])
def test_column_hints(
    src_config: Tuple[dlt.Pipeline, str, str], destination_name: str, init_load: bool
) -> None:
    @dlt.resource
    def tbl_x(data):
        yield data

    @dlt.resource
    def tbl_y(data):
        yield data

    @dlt.resource
    def tbl_z(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    # create three postgres tables
    src_pl.run(
        [
            tbl_x({"id_x": 1, "val_x": "foo", "another_col_x": 1}),
            tbl_y({"id_y": 1, "val_y": "foo", "another_col_y": 1}),
            tbl_z({"id_z": 1, "val_z": "foo", "another_col_z": 1}),
        ]
    )

    # initialize replication and create resources
    column_hints = {
        "tbl_x": {"another_col_x": {"data_type": "double"}},
        "tbl_y": {"another_col_y": {"precision": 32}},
        # tbl_z is not specified, hence all columns should be included
    }
    snapshots = init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y", "tbl_z"),
        publish="insert",
        persist_snapshots=init_load,
        columns=column_hints,
    )
    changes = replication_resource(
        slot_name=slot_name, pub_name=pub_name, columns=column_hints
    )

    # update three postgres tables
    src_pl.run(
        [
            tbl_x({"id_x": 2, "val_x": "foo", "another_col_x": 1}),
            tbl_y({"id_y": 2, "val_y": "foo", "another_col_y": 1}),
            tbl_z({"id_z": 2, "val_z": "foo", "another_col_z": 1}),
        ]
    )

    # load to destination and assert column expectations
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, dev_mode=True
    )
    if init_load:
        dest_pl.run(snapshots)
        assert (
            dest_pl.default_schema.get_table_columns("tbl_x")["another_col_x"][
                "data_type"
            ]
            == "double"
        )
        assert (
            dest_pl.default_schema.get_table_columns("tbl_y")["another_col_y"][
                "precision"
            ]
            == 32
        )
        assert (
            dest_pl.default_schema.get_table_columns("tbl_z")["another_col_z"][
                "data_type"
            ]
            == "bigint"
        )
    dest_pl.run(changes)
    assert (
        dest_pl.default_schema.get_table_columns("tbl_x")["another_col_x"]["data_type"]
        == "double"
    )
    assert (
        dest_pl.default_schema.get_table_columns("tbl_y")["another_col_y"]["precision"]
        == 32
    )
    assert (
        dest_pl.default_schema.get_table_columns("tbl_z")["another_col_z"]["data_type"]
        == "bigint"
    )

    # the tests below should pass, but they don't because of a bug that causes
    # column hints to be added to other tables when dispatching to multiple tables
    assert "another_col_x" not in dest_pl.default_schema.get_table_columns("tbl_y")
    assert "another_col_x" not in dest_pl.default_schema.get_table_columns("tbl_z")
    assert "another_col_y" not in dest_pl.default_schema.get_table_columns(
        "tbl_x", include_incomplete=True
    )
    assert "another_col_y" not in dest_pl.default_schema.get_table_columns(
        "tbl_z", include_incomplete=True
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_table_schema_change(
    src_config: Tuple[dlt.Pipeline, str, str], destination_name: str
) -> None:
    src_pl, slot_name, pub_name = src_config

    # create postgres table
    src_pl.run([{"c1": 1, "c2": 1}], table_name="items")

    # initialize replication
    init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="items",
        publish="insert",
    )

    # create resource and pipeline
    changes = replication_resource(slot_name, pub_name)
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, dev_mode=True
    )

    # add a column in one commit, this will create one Relation message
    src_pl.run([{"c1": 2, "c2": 1}, {"c1": 3, "c2": 1, "c3": 1}], table_name="items")
    info = dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "items") == {"items": 2}
    exp = [{"c1": 2, "c2": 1, "c3": None}, {"c1": 3, "c2": 1, "c3": 1}]
    assert_loaded_data(dest_pl, "items", ["c1", "c2", "c3"], exp, "c1")

    # add a column in two commits, this will create two Relation messages
    src_pl.run([{"c1": 4, "c2": 1, "c3": 1}], table_name="items")
    src_pl.run([{"c1": 5, "c2": 1, "c3": 1, "c4": 1}], table_name="items")
    dest_pl.run(changes)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "items") == {"items": 4}
    exp = [
        {"c1": 4, "c2": 1, "c3": 1, "c4": None},
        {"c1": 5, "c2": 1, "c3": 1, "c4": 1},
    ]
    assert_loaded_data(
        dest_pl, "items", ["c1", "c2", "c3", "c4"], exp, "c1", "c1 IN (4, 5)"
    )


def test_init_replication(src_config: Tuple[dlt.Pipeline, str, str]) -> None:
    def get_table_names_in_pub() -> Set[str]:
        with src_pl.sql_client() as c:
            result = c.execute_sql(
                f"SELECT tablename FROM pg_publication_tables WHERE pubname = '{pub_name}';"
            )
            return {tup[0] for tup in result}

    @dlt.resource
    def tbl_x(data):
        yield data

    @dlt.resource
    def tbl_y(data):
        yield data

    @dlt.resource
    def tbl_z(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    # create three postgres tables
    src_pl.run(
        [
            tbl_x({"id_x": 1, "val_x": "foo"}),
            tbl_y({"id_y": 1, "val_y": "foo"}),
            tbl_z({"id_z": 1, "val_z": "foo"}),
        ]
    )
    add_pk(src_pl.sql_client, "tbl_x", "id_x")
    add_pk(src_pl.sql_client, "tbl_y", "id_y")
    add_pk(src_pl.sql_client, "tbl_z", "id_z")

    # initialize replication with a single table
    snapshot = init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="tbl_x",
        persist_snapshots=True,
    )
    assert snapshot is not None
    assert get_table_names_in_pub() == {"tbl_x"}

    # adding another table is supported, but snapshot tables won't be persisted
    snapshots = init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y"),
        persist_snapshots=False,
    )
    assert snapshots is None
    assert get_table_names_in_pub() == {"tbl_x", "tbl_y"}

    # removing a table is not supported
    init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="tbl_x",  # "tbl_y" is no longer provided
    )
    # "tbl_y" is still in the publication
    assert get_table_names_in_pub() == {"tbl_x", "tbl_y"}

    # switching to whole schema replication is supported by omitting `table_names`,
    # but only for Postgres server versions 15 or higher and with superuser privileges
    is_su = is_super_user(src_pl.sql_client)
    if get_pg_version() >= 150000 and is_su:
        init_replication(
            slot_name=slot_name,
            pub_name=pub_name,
            schema_name=src_pl.dataset_name,
        )
        # includes dlt system tables
        assert get_table_names_in_pub() >= {"tbl_x", "tbl_y", "tbl_z"}
    else:
        exp_err = (
            PublicationPrivilegesException
            if not is_su
            else IncompatiblePostgresVersionException
        )
        with pytest.raises(exp_err):
            init_replication(
                slot_name=slot_name,
                pub_name=pub_name,
                schema_name=src_pl.dataset_name,
            )


@pytest.mark.parametrize("initial_snapshot", ["direct", "table"])
def test_tables_added_and_removed(
    src_config: Tuple[dlt.Pipeline, str, str], initial_snapshot: str
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.a (id int PRIMARY KEY, v text);",
        "CREATE TABLE {schema}.b (id int PRIMARY KEY, v text);",
        "INSERT INTO {schema}.a VALUES (1, 'a1');",
        "INSERT INTO {schema}.b VALUES (1, 'b1'), (2, 'b2'), (3, 'b3');",
    )
    schema = src_pl.dataset_name
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert replication_slot_exists(slot_name) is False
    snapshot = init_replication(
        slot_name, pub_name, schema, "a", initial_snapshot="direct"
    )
    assert_load_info(dest_pl.run(snapshot))
    assert replication_slot_exists(slot_name) is True

    # on the existing slot the table is loaded as it is when read
    added = init_replication(
        slot_name, pub_name, schema, "b", initial_snapshot=initial_snapshot
    )
    run_sql(
        src_pl,
        "UPDATE {schema}.a SET v = 'a1 updated' WHERE id = 1;",
        "UPDATE {schema}.b SET v = 'b2 updated' WHERE id = 2;",
    )
    assert_load_info(dest_pl.run(added))
    assert table_rows(dest_pl, "a", ["id", "v"]) == [(1, "a1")]
    run_sql(
        src_pl,
        "DELETE FROM {schema}.b WHERE id = 2;",
        "UPDATE {schema}.b SET v = 'b3 updated' WHERE id = 3;",
    )
    # changes made since the table was added are replayed over the loaded rows
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    assert table_rows(dest_pl, "a", ["id", "v"]) == [(1, "a1 updated")]
    assert table_rows(dest_pl, "b", ["id", "v"]) == [(1, "b1"), (3, "b3 updated")]
    # a published table is loaded again, ie. after a failed load
    reload = init_replication(
        slot_name, pub_name, schema, "b", initial_snapshot=initial_snapshot
    )
    assert_load_info(dest_pl.run(reload))
    assert table_rows(dest_pl, "b", ["id", "v"]) == [(1, "b1"), (3, "b3 updated")]

    # init_replication never removes tables, the owner drops them from the publication
    init_replication(slot_name, pub_name, schema, "a")
    run_sql(
        src_pl,
        f"ALTER PUBLICATION {pub_name} DROP TABLE {{schema}}.b;",
        "INSERT INTO {schema}.a VALUES (2, 'a2');",
        "INSERT INTO {schema}.b VALUES (4, 'b4');",
    )
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    assert table_rows(dest_pl, "a", ["id", "v"]) == [(1, "a1 updated"), (2, "a2")]
    assert table_rows(dest_pl, "b", ["id", "v"]) == [(1, "b1"), (3, "b3 updated")]


def test_init_replication_without_table_ownership(
    src_config: Tuple[dlt.Pipeline, str, str]
) -> None:
    src_pl, slot_name, pub_name = src_config
    if not is_super_user(src_pl.sql_client):
        pytest.skip("Postgres user needs to be superuser to create a role")
    role = f"repl_{uniq_id(6)}"
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, a text);",
        "CREATE TABLE {schema}.others (id int PRIMARY KEY);",
        "INSERT INTO {schema}.items VALUES (1, 'one');",
        f"CREATE ROLE {role} LOGIN REPLICATION PASSWORD '{role}';",
        f"GRANT USAGE ON SCHEMA {{schema}} TO {role};",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {{schema}} TO {role};",
    )
    credentials = dlt.secrets.get(
        "sources.pg_replication.credentials", ConnectionStringCredentials
    )
    credentials.username, credentials.password = role, role
    schema = src_pl.dataset_name
    try:
        # the table owner runs the statement from the error
        with pytest.raises(PublicationPrivilegesException) as exc:
            init_replication(
                slot_name, pub_name, schema, "items", credentials=credentials
            )
        run_sql(src_pl, exc.value.statement)
        with pytest.raises(PublicationPrivilegesException) as exc:
            init_replication(
                slot_name,
                pub_name,
                schema,
                ["items", "others"],
                credentials=credentials,
            )
        assert exc.value.statement.startswith("ALTER PUBLICATION")
        # with the publication in place REPLICATION and SELECT are enough
        snapshot = init_replication(
            slot_name,
            pub_name,
            schema,
            "items",
            credentials=credentials,
            initial_snapshot="direct",
        )
        dest_pl = dlt.pipeline(
            pipeline_name="dest_pl", destination="duckdb", dev_mode=True
        )
        assert_load_info(dest_pl.run(snapshot))
        run_sql(src_pl, "INSERT INTO {schema}.items VALUES (2, 'two');")
        assert_load_info(
            dest_pl.run(
                replication_resource(slot_name, pub_name, credentials=credentials)
            )
        )
        assert table_rows(dest_pl, "items", ["id", "a"]) == [(1, "one"), (2, "two")]
        # reset keeps the publication of another role
        init_replication(
            slot_name, pub_name, schema, "items", credentials=credentials, reset=True
        )
        with src_pl.sql_client() as c:
            assert (
                c.execute_sql(
                    "SELECT count(*) FROM pg_publication WHERE pubname = %s;", pub_name
                )[0][0]
                == 1
            )
        # a snapshot that row-level security would filter fails instead
        run_sql(
            src_pl,
            "ALTER TABLE {schema}.items ENABLE ROW LEVEL SECURITY;",
            "CREATE POLICY only_one ON {schema}.items USING (id = 1);",
        )
        snapshot = init_replication(
            slot_name,
            pub_name,
            schema,
            "items",
            credentials=credentials,
            reset=True,
            initial_snapshot="direct",
        )
        with pytest.raises(PipelineStepFailed, match="row-level security"):
            dest_pl.run(snapshot)
    finally:
        run_sql(
            src_pl,
            f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots"
            f" WHERE slot_name = '{slot_name}';",
            f"DROP OWNED BY {role};",
            f"DROP ROLE {role};",
        )


@pytest.mark.parametrize("initial_snapshot", ["table", "direct"])
def test_generated_columns_not_in_snapshot(
    src_config: Tuple[dlt.Pipeline, str, str], initial_snapshot: str
) -> None:
    if get_pg_version() < 120000:
        pytest.skip("generated columns need Postgres 12")
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, a int,"
        " doubled int GENERATED ALWAYS AS (a * 2) STORED);",
        "INSERT INTO {schema}.items (id, a) VALUES (1, 1), (2, 2);",
    )
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "items",
        initial_snapshot=initial_snapshot,
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(snapshot))
    run_sql(src_pl, "UPDATE {schema}.items SET a = 10 WHERE id = 1;")
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    # Postgres does not replicate them, updated rows would get NULL
    assert "doubled" not in dest_pl.default_schema.get_table_columns("items")
    assert table_rows(dest_pl, "items", ["id", "a"]) == [(1, 10), (2, 2)]


@pytest.mark.parametrize("naming", ["snake_case", "sql_cs_v1"])
def test_names_that_differ_in_case(
    src_config: Tuple[dlt.Pipeline, str, str], naming: str
) -> None:
    src_pl, slot_name, pub_name = src_config
    os.environ["SCHEMA__NAMING"] = naming
    create_tables(
        src_pl,
        'CREATE TABLE {schema}.items (id int PRIMARY KEY, "ID" text);',
        'CREATE TABLE {schema}."Items" (id int PRIMARY KEY);',
        "INSERT INTO {schema}.items VALUES (1, 'one');",
        'INSERT INTO {schema}."Items" VALUES (10);',
    )
    schema = src_pl.dataset_name
    if naming == "snake_case":
        with pytest.raises(ColumnNameCollisionException):
            init_replication(slot_name, pub_name, schema, "items", publish="insert")
        with pytest.raises(TableNameCollisionException):
            init_replication(
                slot_name, pub_name, schema, ["items", "Items"], publish="insert"
            )
        return
    pq = pytest.importorskip("pyarrow.parquet")
    # duckdb identifiers are case insensitive, files keep both names
    bucket_url = os.path.abspath(os.path.join(TEST_STORAGE_ROOT, "files"))
    os.makedirs(bucket_url, exist_ok=True)
    if os.path.exists(bucket_url.upper()):
        pytest.skip("the file system does not tell Items and items apart")
    snapshot = init_replication(
        slot_name,
        pub_name,
        schema,
        ["items", "Items"],
        publish="insert",
        initial_snapshot="direct",
    )
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl",
        destination=dlt.destinations.filesystem(bucket_url),
        dev_mode=True,
    )
    assert_load_info(dest_pl.run(snapshot, loader_file_format="parquet"))
    run_sql(
        src_pl,
        "INSERT INTO {schema}.items VALUES (2, 'two');",
        'INSERT INTO {schema}."Items" VALUES (20);',
    )
    assert_load_info(
        dest_pl.run(
            replication_resource(slot_name, pub_name), loader_file_format="parquet"
        )
    )

    def file_rows(table_name: str, columns: List[str]) -> List[Tuple[Any, ...]]:
        pattern = os.path.join(
            bucket_url, dest_pl.dataset_name, table_name, "*.parquet"
        )
        rows = [
            tuple(row[c] for c in columns)
            for path in glob.glob(pattern)
            for row in pq.read_table(path).to_pylist()
        ]
        return sorted(rows)

    assert file_rows("items", ["id", "ID"]) == [(1, "one"), (2, "two")]
    assert file_rows("Items", ["id"]) == [(10,), (20,)]


def test_replicate_schema(src_config: Tuple[dlt.Pipeline, str, str]) -> None:
    if get_pg_version() < 150000:
        pytest.skip("incompatible Postgres server version")
    if not is_super_user(src_config[0].sql_client):
        pytest.skip("Postgres user needs to be superuser")

    @dlt.resource
    def tbl_x(data):
        yield data

    @dlt.resource
    def tbl_y(data):
        yield data

    @dlt.resource
    def tbl_z(data):
        yield data

    src_pl, slot_name, pub_name = src_config

    # create two postgres tables
    src_pl.run(
        [
            tbl_x({"id_x": 1, "val_x": "foo"}),
            tbl_y({"id_y": 1, "val_y": "foo"}),
        ]
    )

    # initialize replication and create resource
    init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,  # we only specify `schema_name`, not `table_names`
        publish="insert",
    )
    changes = replication_resource(slot_name, pub_name)

    # change source tables and load to destination
    src_pl.run(
        [
            tbl_x({"id_x": 2, "val_x": "foo"}),
            tbl_y({"id_y": 2, "val_y": "foo"}),
        ]
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", dev_mode=True)
    dest_pl.extract(changes)
    assert set(dest_pl.default_schema.data_table_names()) == {"tbl_x", "tbl_y"}

    # introduce new table in source and assert it gets included in the replication
    src_pl.run(
        [
            tbl_x({"id_x": 3, "val_x": "foo"}),
            tbl_y({"id_y": 3, "val_y": "foo"}),
            tbl_z({"id_z": 1, "val_z": "foo"}),
        ]
    )
    dest_pl.extract(changes)
    assert set(dest_pl.default_schema.data_table_names()) == {"tbl_x", "tbl_y", "tbl_z"}


def test_batching(src_config: Tuple[dlt.Pipeline, str, str]) -> None:
    # this test asserts the number of data items yielded by the replication resource
    # is not affected by `target_batch_size` and the number of replication messages per transaction
    src_pl, slot_name, pub_name = src_config

    # create postgres table with single record
    data = {"id": 1000, "val": True}
    src_pl.run([data], table_name="items")
    add_pk(src_pl.sql_client, "items", "id")

    # initialize replication and create resource for changes
    init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="items",
    )
    changes = replication_resource(slot_name, pub_name, target_batch_size=50)

    # create destination pipeline and resource
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", dev_mode=True)

    # insert 100 records into source table in one transaction
    batch = [{**r, **{"id": key}} for r in [data] for key in range(1, 101)]
    src_pl.run(batch, table_name="items")
    extract_info = dest_pl.extract(changes)
    assert extract_info.asdict()["job_metrics"][0]["items_count"] == 100

    # insert 100 records into source table in 5 transactions
    batch = [{**r, **{"id": key}} for r in [data] for key in range(101, 121)]
    src_pl.run(batch, table_name="items")
    batch = [{**r, **{"id": key}} for r in [data] for key in range(121, 141)]
    src_pl.run(batch, table_name="items")
    batch = [{**r, **{"id": key}} for r in [data] for key in range(141, 161)]
    src_pl.run(batch, table_name="items")
    batch = [{**r, **{"id": key}} for r in [data] for key in range(161, 181)]
    src_pl.run(batch, table_name="items")
    batch = [{**r, **{"id": key}} for r in [data] for key in range(181, 201)]
    src_pl.run(batch, table_name="items")
    extract_info = dest_pl.extract(changes)
    assert extract_info.asdict()["job_metrics"][0]["items_count"] == 100


def _extract_with_timeout(
    pipeline: dlt.Pipeline, data: Any, timeout: float = 60.0
) -> Any:
    result: List[Any] = []

    def _extract() -> None:
        try:
            result.append(pipeline.extract(data))
        except Exception as ex:
            result.append(ex)

    thread = threading.Thread(target=_extract, daemon=True)
    thread.start()
    thread.join(timeout)
    assert not thread.is_alive(), f"extract did not finish in {timeout} seconds"
    if isinstance(result[0], Exception):
        raise result[0]
    return result[0]


def test_multi_batch_with_backlog(
    src_config: Tuple[dlt.Pipeline, str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    src_pl, slot_name, pub_name = src_config
    rep_conns: List[Any] = []

    def _count_rep_conn(credentials: ConnectionStringCredentials) -> Any:
        rep_conns.append(get_rep_conn(credentials))
        return rep_conns[-1]

    get_rep_conn = helpers._get_rep_conn
    monkeypatch.setattr(helpers, "_get_rep_conn", _count_rep_conn)
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY);",
        "CREATE TABLE {schema}.noise (id int);",
    )
    init_replication(slot_name, pub_name, src_pl.dataset_name, "items")
    # unpublished changes keep the server decoding while batches are yielded
    run_sql(
        src_pl,
        *(
            f"INSERT INTO {{schema}}.items SELECT generate_series({i * 100 + 1}, {i * 100 + 100});"
            " INSERT INTO {schema}.noise SELECT generate_series(1, 2000);"
            for i in range(300)
        ),
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", dev_mode=True)
    rep_conns.clear()
    extract_info = _extract_with_timeout(
        dest_pl, replication_resource(slot_name, pub_name, target_batch_size=100)
    )
    assert get_table_metrics(extract_info, "items")["items_count"] == 30000
    # reconnecting per batch races with the previous walsender and decodes the WAL again
    assert len(rep_conns) == 1


def test_no_new_changes_without_flush(
    src_config: Tuple[dlt.Pipeline, str, str]
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(src_pl, "CREATE TABLE {schema}.items (id int PRIMARY KEY);")
    init_replication(slot_name, pub_name, src_pl.dataset_name, "items")
    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (1);")
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", dev_mode=True)
    extract_info = _extract_with_timeout(
        dest_pl, replication_resource(slot_name, pub_name, flush_slot=False)
    )
    assert get_table_metrics(extract_info, "items")["items_count"] == 1
    # the second run has nothing to read and must stop at the WAL position from its start
    extract_info = _extract_with_timeout(
        dest_pl, replication_resource(slot_name, pub_name, flush_slot=False)
    )
    assert get_table_metrics(extract_info, "items") is None


def test_add_limit_makes_progress(src_config: Tuple[dlt.Pipeline, str, str]) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(src_pl, "CREATE TABLE {schema}.items (id int PRIMARY KEY);")
    init_replication(slot_name, pub_name, src_pl.dataset_name, "items")
    run_sql(
        src_pl,
        *(
            f"INSERT INTO {{schema}}.items SELECT generate_series({i * 100 + 1}, {i * 100 + 100});"
            for i in range(10)
        ),
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    for expected_count in (200, 400, 600):
        # a batch is a hints item and a data item, so 4 items are 2 batches
        changes = replication_resource(
            slot_name, pub_name, target_batch_size=100
        ).add_limit(4)
        assert_load_info(dest_pl.run(changes))
        assert load_table_counts(dest_pl, "items") == {"items": expected_count}
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    assert load_table_counts(dest_pl, "items") == {"items": 1000}


def test_changes_of_lost_run_load_again(
    src_config: Tuple[dlt.Pipeline, str, str]
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, a text);",
        "INSERT INTO {schema}.items VALUES (1, 'one'), (4, 'four');",
    )
    snapshot = init_replication(
        slot_name, pub_name, src_pl.dataset_name, "items", initial_snapshot="direct"
    )
    # outside of the working dir, which the test drops
    destination = dlt.destinations.duckdb("lost_run.duckdb")
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination, dev_mode=True
    )
    assert_load_info(dest_pl.run(snapshot))
    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (2, 'two');")
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    run_sql(
        src_pl,
        "INSERT INTO {schema}.items VALUES (3, 'three');",
        "UPDATE {schema}.items SET a = 'uno' WHERE id = 1;",
        "DELETE FROM {schema}.items WHERE id = 2;",
    )
    # the run stops before loading and its working dir is lost, ie. a container without volume
    dest_pl.extract(replication_resource(slot_name, pub_name))
    dataset_name = dest_pl.dataset_name
    dest_pl.drop()
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination, dataset_name=dataset_name
    )
    dest_pl.sync_destination()
    assert "items" in dest_pl.default_schema.tables
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    assert table_rows(dest_pl, "items", ["id", "a"]) == [
        (1, "uno"),
        (3, "three"),
        (4, "four"),
    ]


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
def test_skip_transaction_with_slot_advance(
    src_config: Tuple[dlt.Pipeline, str, str], backend: str
) -> None:
    if get_pg_version() < 110000:
        pytest.skip("pg_replication_slot_advance needs Postgres 11")
    src_pl, slot_name, pub_name = src_config
    create_tables(src_pl, "CREATE TABLE {schema}.items (id int PRIMARY KEY, d date);")
    init_replication(slot_name, pub_name, src_pl.dataset_name, "items")
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (1, '2024-01-01');")
    assert_load_info(
        dest_pl.run(replication_resource(slot_name, pub_name, backend=backend))
    )
    run_sql(
        src_pl,
        # python dates have no years before 1
        "INSERT INTO {schema}.items VALUES (2, '0044-03-15 BC');",
        "INSERT INTO {schema}.items VALUES (3, '2024-01-03');",
    )
    with pytest.raises(PipelineStepFailed) as exc:
        dest_pl.run(replication_resource(slot_name, pub_name, backend=backend))
    error = exc.value.exception.__cause__
    assert isinstance(error, ValueConversionException)
    assert error.column_name == "d"
    # the statement from the error skips only the transaction with the value
    run_sql(
        src_pl,
        f"SELECT pg_replication_slot_advance('{slot_name}', '{error.position.skip_lsn}');",
    )
    assert_load_info(
        dest_pl.run(replication_resource(slot_name, pub_name, backend=backend))
    )
    assert table_rows(dest_pl, "items", ["id"]) == [(1,), (3,)]


@pytest.mark.parametrize(
    "publish", ["insert", "insert, update, delete"], ids=["append", "merge"]
)
@pytest.mark.parametrize(
    "in_first_transaction", [True, False], ids=["first-txn", "later-txn"]
)
def test_schema_change_mid_transaction(
    src_config: Tuple[dlt.Pipeline, str, str], publish: str, in_first_transaction: bool
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.x (id int PRIMARY KEY);",
        "CREATE TABLE {schema}.y (id int PRIMARY KEY);",
    )
    init_replication(
        slot_name, pub_name, src_pl.dataset_name, ("x", "y"), publish=publish
    )
    if not in_first_transaction:
        run_sql(
            src_pl,
            "INSERT INTO {schema}.x VALUES (0); INSERT INTO {schema}.y VALUES (1);",
        )
    # x gets a second Relation message in the middle of the transaction
    run_sql(
        src_pl,
        "BEGIN; INSERT INTO {schema}.x VALUES (1); INSERT INTO {schema}.y VALUES (2);"
        " ALTER TABLE {schema}.x ADD COLUMN c int; INSERT INTO {schema}.x VALUES (2, 2); COMMIT;",
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    exp_x = (
        [(1, None), (2, 2)] if in_first_transaction else [(0, None), (1, None), (2, 2)]
    )
    exp_y = [(2,)] if in_first_transaction else [(1,), (2,)]
    assert table_rows(dest_pl, "x", ["id", "c"]) == exp_x
    assert table_rows(dest_pl, "y", ["id"]) == exp_y


def test_primary_key_update(src_config: Tuple[dlt.Pipeline, str, str]) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(src_pl, "CREATE TABLE {schema}.items (id int PRIMARY KEY, val text);")
    init_replication(slot_name, pub_name, src_pl.dataset_name, "items")
    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (1, 'a'), (2, 'b');")
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    run_sql(src_pl, "UPDATE {schema}.items SET id = 10 WHERE id = 1;")
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    assert table_rows(dest_pl, "items", ["id", "val"]) == [(2, "b"), (10, "a")]


@pytest.mark.parametrize(
    "replica_identity,table_key,initial_snapshot,backend",
    [
        ("DEFAULT", ["a", "b"], "table", "python"),
        ("DEFAULT", ["a", "b"], "direct", "pyarrow"),
        ("FULL", ["a", "b"], "unlogged_table", "pyarrow"),
        # snapshots merge on the primary key, replication on the index, both identify a row
        ("USING INDEX items_ab", ["id"], "direct", "python"),
    ],
    ids=["default-table", "default-direct", "full-unlogged_table", "index-direct"],
)
def test_compound_primary_key(
    src_config: Tuple[dlt.Pipeline, str, str],
    replica_identity: str,
    table_key: List[str],
    initial_snapshot: str,
    backend: str,
) -> None:
    def primary_key() -> List[str]:
        columns = dest_pl.default_schema.get_table_columns("items")
        return [name for name, c in columns.items() if c.get("primary_key")]

    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id serial, a int NOT NULL, b int NOT NULL, val text);",
        f"ALTER TABLE {{schema}}.items ADD PRIMARY KEY ({', '.join(table_key)});",
        "CREATE UNIQUE INDEX items_ab ON {schema}.items (a, b);",
        f"ALTER TABLE {{schema}}.items REPLICA IDENTITY {replica_identity};",
        "INSERT INTO {schema}.items (a, b, val) VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z');",
    )
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "items",
        initial_snapshot=initial_snapshot,
        backend=backend,
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(snapshot))
    assert primary_key() == table_key

    run_sql(
        src_pl,
        "UPDATE {schema}.items SET val = 'x2' WHERE a = 1 AND b = 1;",
        "UPDATE {schema}.items SET b = 3 WHERE a = 1 AND b = 2;",
        "DELETE FROM {schema}.items WHERE a = 2 AND b = 1;",
        "INSERT INTO {schema}.items (a, b, val) VALUES (2, 2, 'w');",
    )
    changes = replication_resource(slot_name, pub_name, backend=backend)
    assert_load_info(dest_pl.run(changes))
    assert primary_key() == ["a", "b"]
    assert table_rows(dest_pl, "items", ["a", "b", "val"], order_by="a, b") == [
        (1, 1, "x2"),
        (1, 3, "y"),
        (2, 2, "w"),
    ]


@pytest.mark.parametrize(
    "items_ddl",
    [
        "CREATE TABLE {schema}.items (id int, val text);",
        "CREATE TABLE {schema}.items (id int, val text);"
        " ALTER TABLE {schema}.items REPLICA IDENTITY FULL;",
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, val text);"
        " ALTER TABLE {schema}.items REPLICA IDENTITY NOTHING;",
    ],
    ids=["default", "full", "nothing_with_pk"],
)
def test_table_without_primary_key(
    src_config: Tuple[dlt.Pipeline, str, str], items_ddl: str
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl, items_ddl, "CREATE TABLE {schema}.keyed (id int PRIMARY KEY);"
    )
    with pytest.raises(NoPrimaryKeyException) as exc:
        init_replication(
            slot_name,
            pub_name,
            src_pl.dataset_name,
            ("items", "keyed"),
            initial_snapshot="table",
        )
    assert exc.value.table_names == ["items"]
    # nothing was published, so the source still accepts updates and deletes
    with src_pl.sql_client() as c:
        assert not c.execute_sql(
            "SELECT 1 FROM pg_publication_tables WHERE pubname = %s;", pub_name
        )
    run_sql(
        src_pl,
        "INSERT INTO {schema}.items VALUES (1, 'a');",
        "UPDATE {schema}.items SET val = 'b';",
        "DELETE FROM {schema}.items;",
    )


def test_replicated_table_loses_primary_key(
    src_config: Tuple[dlt.Pipeline, str, str]
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, val text);",
        "ALTER TABLE {schema}.items REPLICA IDENTITY FULL;",
    )
    init_replication(slot_name, pub_name, src_pl.dataset_name, "items")
    run_sql(
        src_pl,
        "ALTER TABLE {schema}.items DROP CONSTRAINT items_pkey;",
        "INSERT INTO {schema}.items VALUES (1, 'a'), (1, 'a');",
        "UPDATE {schema}.items SET val = 'b';",
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    with pytest.raises(PipelineStepFailed) as exc:
        dest_pl.run(replication_resource(slot_name, pub_name))
    assert "no primary key or replica identity index" in str(exc.value)


def test_unchanged_toast_with_replica_identity_full(
    src_config: Tuple[dlt.Pipeline, str, str]
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, small text, big text);",
        # store large values out of line without compression
        "ALTER TABLE {schema}.items ALTER COLUMN big SET STORAGE EXTERNAL;",
        "ALTER TABLE {schema}.items REPLICA IDENTITY FULL;",
    )
    init_replication(slot_name, pub_name, src_pl.dataset_name, "items")
    run_sql(
        src_pl,
        "INSERT INTO {schema}.items VALUES (1, 'a', repeat('x', 10000)), (2, 'a', repeat('y', 5000));",
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    run_sql(
        src_pl,
        "UPDATE {schema}.items SET small = 'b';",
        "UPDATE {schema}.items SET id = 20 WHERE id = 2;",
    )
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    assert table_rows(dest_pl, "items", ["id", "small", "length(big)"]) == [
        (1, "b", 10000),
        (20, "b", 5000),
    ]
    columns = dest_pl.default_schema.get_table_columns("items")
    assert [name for name, c in columns.items() if c.get("primary_key")] == ["id"]


def test_unchanged_toast_without_replica_identity_full(
    src_config: Tuple[dlt.Pipeline, str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, small text, big text);",
        "ALTER TABLE {schema}.items ALTER COLUMN big SET STORAGE EXTERNAL;",
    )
    warnings: List[str] = []
    monkeypatch.setattr(helpers.logger, "warning", warnings.append)
    init_replication(slot_name, pub_name, src_pl.dataset_name, "items")
    assert any("REPLICA IDENTITY FULL" in w and "small, big" in w for w in warnings)

    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (1, 'a', repeat('x', 10000));")
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    run_sql(src_pl, "UPDATE {schema}.items SET small = 'b';")
    with pytest.raises(PipelineStepFailed) as exc:
        dest_pl.run(replication_resource(slot_name, pub_name))
    assert "REPLICA IDENTITY FULL" in str(exc.value)


@pytest.mark.parametrize(
    "initial_snapshot,backend", [("table", "python"), ("direct", "pyarrow")]
)
def test_exclude_unchanged_toast_columns(
    src_config: Tuple[dlt.Pipeline, str, str],
    monkeypatch: pytest.MonkeyPatch,
    initial_snapshot: str,
    backend: str,
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, small text, big text);",
        "ALTER TABLE {schema}.items ALTER COLUMN big SET STORAGE EXTERNAL;",
        "INSERT INTO {schema}.items VALUES (1, 'a', repeat('x', 10000));",
    )
    exclude_columns = {"items": ["big"]}
    warnings: List[str] = []
    monkeypatch.setattr(helpers.logger, "warning", warnings.append)
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "items",
        exclude_columns=exclude_columns,
        initial_snapshot=initial_snapshot,
        backend=backend,
    )
    (warning,) = [w for w in warnings if "REPLICA IDENTITY FULL" in w]
    assert "columns small may" in warning

    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(snapshot))
    run_sql(
        src_pl,
        "UPDATE {schema}.items SET small = 'b';",
        "INSERT INTO {schema}.items VALUES (2, 'c', repeat('y', 10000));",
    )
    changes = replication_resource(
        slot_name, pub_name, exclude_columns=exclude_columns, backend=backend
    )
    assert_load_info(dest_pl.run(changes))
    assert table_rows(dest_pl, "items", ["id", "small"]) == [(1, "b"), (2, "c")]
    assert "big" not in dest_pl.default_schema.get_table_columns("items")


@pytest.mark.parametrize(
    "session_options",
    [
        None,
        "-c datestyle=SQL,DMY -c intervalstyle=iso_8601 -c extra_float_digits=0"
        " -c timezone=Asia/Kolkata",
    ],
    ids=["default_session", "other_session"],
)
@pytest.mark.parametrize(
    "initial_snapshot,backend",
    [
        ("table", "python"),
        ("unlogged_table", "pyarrow"),
        ("direct", "python"),
        ("direct", "pyarrow"),
    ],
)
@pytest.mark.parametrize("reflection_level", ["full", "full_with_precision"])
def test_snapshot_types_match_replication(
    src_config: Tuple[dlt.Pipeline, str, str],
    monkeypatch: pytest.MonkeyPatch,
    initial_snapshot: str,
    backend: str,
    session_options: str,
    reflection_level: TReflectionLevel,
) -> None:
    src_pl, slot_name, pub_name = src_config
    if session_options:
        # settings in the connection string, like role or database defaults
        credentials = dlt.secrets.get(
            "sources.pg_replication.credentials", ConnectionStringCredentials
        )
        credentials.query = {**(credentials.query or {}), "options": session_options}
        monkeypatch.setenv(
            "SOURCES__PG_REPLICATION__CREDENTIALS",
            credentials.to_native_representation(),
        )
    create_tables(src_pl, PG_TYPES_DDL)
    run_sql(src_pl, f"INSERT INTO {{schema}}.items VALUES {PG_TYPES_ROW.format(id=1)};")
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "items",
        initial_snapshot=initial_snapshot,
        backend=backend,
        reflection_level=reflection_level,
    )
    run_sql(src_pl, f"INSERT INTO {{schema}}.items VALUES {PG_TYPES_ROW.format(id=2)};")

    snapshot_pl = dlt.pipeline(
        pipeline_name="snapshot_pl", destination="duckdb", dev_mode=True
    )
    changes_pl = dlt.pipeline(
        pipeline_name="changes_pl", destination="duckdb", dev_mode=True
    )
    assert_load_info(snapshot_pl.run(snapshot))
    if initial_snapshot == "direct":
        assert snapshot.closed
    assert_load_info(
        changes_pl.run(
            replication_resource(
                slot_name, pub_name, backend=backend, reflection_level=reflection_level
            )
        )
    )

    snapshot_columns = snapshot_pl.default_schema.get_table_columns("items")
    changes_columns = changes_pl.default_schema.get_table_columns("items")
    column_names = [name for name in snapshot_columns if not name.startswith("_dlt")]
    for name in column_names:
        hints = ("data_type", "precision", "scale", "timezone")
        assert {h: snapshot_columns[name].get(h) for h in hints} == {
            h: changes_columns[name].get(h) for h in hints
        }, name
    column_names.remove("id")
    assert table_rows(snapshot_pl, "items", column_names) == table_rows(
        changes_pl, "items", column_names
    )

    # changes load on top of the snapshot
    assert_load_info(
        snapshot_pl.run(replication_resource(slot_name, pub_name, backend=backend))
    )
    assert load_table_counts(snapshot_pl, "items") == {"items": 2}
    assert not [
        c for c in snapshot_pl.default_schema.get_table_columns("items") if "__v_" in c
    ]


@pytest.mark.parametrize("initial_snapshot", ["table", "direct"])
def test_unbounded_numeric_without_precision_fails_on_pyarrow(
    src_config: Tuple[dlt.Pipeline, str, str], initial_snapshot: str
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, n numeric);",
        "INSERT INTO {schema}.items VALUES (1, 1.1234567890123);",
    )
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "items",
        initial_snapshot=initial_snapshot,
        backend="pyarrow",
    )
    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (2, 2.1234567890123);")
    # the default decimal(38, 9) cannot hold the value
    for idx, resource in enumerate(
        [snapshot, replication_resource(slot_name, pub_name, backend="pyarrow")]
    ):
        dest_pl = dlt.pipeline(
            pipeline_name=f"dest_pl_{idx}", destination="duckdb", dev_mode=True
        )
        with pytest.raises(
            PipelineStepFailed, match="Conversion to arrow failed for field `n`"
        ):
            dest_pl.run(resource)


@pytest.mark.parametrize(
    "initial_snapshot,backend",
    [
        ("table", "python"),
        ("table", "pyarrow"),
        ("direct", "python"),
        ("direct", "pyarrow"),
    ],
)
def test_unbounded_numeric_with_precision_hint(
    src_config: Tuple[dlt.Pipeline, str, str], initial_snapshot: str, backend: str
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, n numeric);",
        "INSERT INTO {schema}.items VALUES (1, 1.1234567890123);",
    )
    columns: Dict[str, TTableSchemaColumns] = {
        "items": {"n": {"data_type": "decimal", "precision": 38, "scale": 13}}
    }
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "items",
        columns=columns,
        initial_snapshot=initial_snapshot,
        backend=backend,
    )
    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (2, 2.1234567890123);")
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(snapshot))
    assert_load_info(
        dest_pl.run(
            replication_resource(slot_name, pub_name, columns=columns, backend=backend)
        )
    )
    assert table_rows(dest_pl, "items", ["id", "n"]) == [
        (1, Decimal("1.1234567890123")),
        (2, Decimal("2.1234567890123")),
    ]


TYPE_CHANGES = {
    "int_to_bigint": ("n", "bigint", "9000000000", 9000000000),
    "varchar_longer": ("v", "varchar(50)", "'longer than five'", "longer than five"),
    "jsonb_to_json": ("j", "json", "'[1]'", None),
    "numeric_more_scale": ("m", "numeric(6,3)", "1.125", Decimal("1.125")),
    "int_to_numeric": ("n", "numeric(12,2)", "1.5", Decimal("1.5")),
    "int_to_text": ("n", "text", "'six'", "six"),
    "timestamp_to_timestamptz": (
        "ts",
        "timestamptz",
        "'2024-01-02 10:11:12+00'",
        datetime(2024, 1, 2, 10, 11, 12, tzinfo=timezone.utc),
    ),
}
"""Column, new type, inserted literal and the loaded value, None if not compared."""

TYPE_CHANGES_THAT_DO_NOT_FIT = [
    ("varchar_longer", "full_with_precision"),
    ("numeric_more_scale", "full"),
    ("int_to_numeric", "full"),
    ("int_to_text", "full"),
    ("timestamp_to_timestamptz", "full"),
]


def snapshot_then_change_type(
    src_config: Tuple[dlt.Pipeline, str, str],
    destination_name: str,
    change: str,
    reflection_level: TReflectionLevel,
) -> dlt.Pipeline:
    """Loads a snapshot of `events`, then changes the type of a column and inserts a row."""
    src_pl, slot_name, pub_name = src_config
    column, new_type, literal, _ = TYPE_CHANGES[change]
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.events (id int PRIMARY KEY, n int, v varchar(5),"
        " m numeric(7,2), j jsonb, ts timestamp);",
        "INSERT INTO {schema}.events VALUES (1, 1, 'one', 1.25, '{\"a\": 1}', '2024-01-01');",
    )
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "events",
        initial_snapshot="direct",
        reflection_level=reflection_level,
    )
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, dev_mode=True
    )
    assert_load_info(dest_pl.run(snapshot))
    run_sql(
        src_pl,
        f"ALTER TABLE {{schema}}.events ALTER COLUMN {column} TYPE {new_type}"
        f" USING {column}::{new_type};",
        f"INSERT INTO {{schema}}.events (id, {column}) VALUES (2, {literal});",
    )
    return dest_pl


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize(
    "change,reflection_level,fits",
    [
        ("int_to_bigint", "full", True),
        ("varchar_longer", "full", True),
        ("jsonb_to_json", "full", True),
    ]
    + [(change, level, False) for change, level in TYPE_CHANGES_THAT_DO_NOT_FIT],
)
def test_column_type_change(
    src_config: Tuple[dlt.Pipeline, str, str],
    destination_name: str,
    change: str,
    reflection_level: TReflectionLevel,
    fits: bool,
) -> None:
    _, slot_name, pub_name = src_config
    column, _, _, value = TYPE_CHANGES[change]
    dest_pl = snapshot_then_change_type(
        src_config, destination_name, change, reflection_level
    )
    snapshot_column = dest_pl.default_schema.get_table_columns("events")[column]
    changes = replication_resource(
        slot_name, pub_name, reflection_level=reflection_level
    )
    if fits:
        assert_load_info(dest_pl.run(changes))
        loaded_column = dest_pl.default_schema.get_table_columns("events")[column]
        for hint in ("data_type", "precision", "scale", "timezone"):
            assert loaded_column.get(hint) == snapshot_column.get(hint)  # type: ignore[misc]
        if value is not None:
            assert table_rows(dest_pl, "events", ["id", column])[-1] == (2, value)
        return
    # stops before loading, also in the next run
    for _ in range(2):
        with pytest.raises(PipelineStepFailed) as exc:
            dest_pl.run(changes)
        assert "does not fit the destination column" in str(exc.value)
        assert load_table_counts(dest_pl, "events") == {"events": 1}
    assert_load_info(
        dest_pl.run(
            replication_resource(
                slot_name, pub_name, exclude_columns={"events": [column]}
            )
        )
    )
    assert table_rows(dest_pl, "events", ["id"]) == [(1,), (2,)]


@pytest.mark.parametrize("change,reflection_level", TYPE_CHANGES_THAT_DO_NOT_FIT)
def test_column_type_change_with_suggested_columns(
    src_config: Tuple[dlt.Pipeline, str, str],
    change: str,
    reflection_level: TReflectionLevel,
) -> None:
    src_pl, slot_name, pub_name = src_config
    column, _, literal, value = TYPE_CHANGES[change]
    dest_pl = snapshot_then_change_type(src_config, "duckdb", change, reflection_level)
    with pytest.raises(PipelineStepFailed) as exc:
        dest_pl.run(
            replication_resource(slot_name, pub_name, reflection_level=reflection_level)
        )
    cause = exc.value.exception.__cause__
    assert isinstance(cause, IncompatibleColumnTypeException)
    suggested = cause.columns
    # do what the message says: change the destination column, pass the suggested `columns`
    with dest_pl.destination_client() as client:
        db_type = client.type_mapper.to_destination_type(  # type: ignore[attr-defined]
            {"name": column, **suggested["events"][column]},
            dest_pl.default_schema.get_table("events"),
        )
    with dest_pl.sql_client() as c:
        c.execute_sql(
            f"ALTER TABLE {c.make_qualified_table_name('events')} ALTER COLUMN"
            f" {c.escape_column_name(column)} TYPE {db_type};"
        )
        # merge loads into the staging table first, it is created again with the new type
        with c.with_staging_dataset():
            c.drop_tables("events")
    assert_load_info(
        dest_pl.run(
            replication_resource(
                slot_name,
                pub_name,
                reflection_level=reflection_level,
                columns=suggested,
            )
        )
    )
    assert table_rows(dest_pl, "events", ["id", column])[-1] == (2, value)
    # later runs keep the new type without the hint
    run_sql(
        src_pl, f"INSERT INTO {{schema}}.events (id, {column}) VALUES (3, {literal});"
    )
    assert_load_info(
        dest_pl.run(
            replication_resource(slot_name, pub_name, reflection_level=reflection_level)
        )
    )
    assert table_rows(dest_pl, "events", ["id", column])[-1] == (3, value)


def test_snapshot_in_other_pipeline(src_config: Tuple[dlt.Pipeline, str, str]) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.events (id int PRIMARY KEY, n int);",
        "INSERT INTO {schema}.events VALUES (1, 1);",
    )
    snapshot = init_replication(
        slot_name, pub_name, src_pl.dataset_name, "events", initial_snapshot="direct"
    )
    snapshot_pl = dlt.pipeline(
        pipeline_name="snapshot_pl", destination="duckdb", dev_mode=True
    )
    assert_load_info(snapshot_pl.run(snapshot))
    # the type changes before the first replication run
    run_sql(
        src_pl,
        "ALTER TABLE {schema}.events ALTER COLUMN n TYPE text;",
        "INSERT INTO {schema}.events VALUES (2, 'two');",
    )
    empty_pl = dlt.pipeline(
        pipeline_name="empty_pl", destination="duckdb", dev_mode=True
    )
    changes_pl = dlt.pipeline(
        pipeline_name="changes_pl", destination="duckdb", dev_mode=True
    )
    with pytest.raises(PipelineStepFailed, match="sync_destination"):
        changes_pl.run(
            replication_resource(slot_name, pub_name, snapshot_pipeline=empty_pl)
        )
    with pytest.raises(PipelineStepFailed, match="does not fit the destination column"):
        changes_pl.run(
            replication_resource(slot_name, pub_name, snapshot_pipeline=snapshot_pl)
        )
    # without the snapshot schema the types come from the Relation message
    assert_load_info(changes_pl.run(replication_resource(slot_name, pub_name)))
    assert (
        changes_pl.default_schema.get_table_columns("events")["n"]["data_type"]
        == "text"
    )
    # only the first run takes the types from the snapshot pipeline
    run_sql(src_pl, "INSERT INTO {schema}.events VALUES (3, 'three');")
    assert_load_info(
        changes_pl.run(
            replication_resource(slot_name, pub_name, snapshot_pipeline=snapshot_pl)
        )
    )
    assert table_rows(changes_pl, "events", ["id", "n"])[-1] == (3, "three")


@pytest.mark.parametrize("initial_snapshot", [None, "table", "direct"])
@pytest.mark.parametrize(
    "reflect_not_null,change,stops",
    [
        (False, "rename", False),
        (True, "rename", True),
        (True, "drop_not_null", True),
    ],
)
def test_not_null(
    src_config: Tuple[dlt.Pipeline, str, str],
    initial_snapshot: Optional[str],
    reflect_not_null: bool,
    change: str,
    stops: bool,
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, a text NOT NULL, b int);",
        "INSERT INTO {schema}.items VALUES (1, 'one', 1);",
    )
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "items",
        initial_snapshot=initial_snapshot,
        reflect_not_null=reflect_not_null,
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    changes = replication_resource(
        slot_name, pub_name, reflect_not_null=reflect_not_null
    )
    if initial_snapshot:
        assert_load_info(dest_pl.run(snapshot))
    else:
        # the replication creates the table
        run_sql(src_pl, "UPDATE {schema}.items SET b = 2 WHERE id = 1;")
        assert_load_info(dest_pl.run(changes))
    columns = dest_pl.default_schema.get_table_columns("items")
    assert columns["a"].get("nullable", True) is not reflect_not_null
    if change == "rename":
        run_sql(
            src_pl,
            "ALTER TABLE {schema}.items RENAME COLUMN a TO c;",
            "INSERT INTO {schema}.items VALUES (2, 'two', 2);",
        )
    else:
        run_sql(
            src_pl,
            "ALTER TABLE {schema}.items ALTER COLUMN a DROP NOT NULL;",
            "INSERT INTO {schema}.items VALUES (2, NULL, 2);",
        )
    if stops:
        with pytest.raises(PipelineStepFailed, match="are NOT NULL in the destination"):
            dest_pl.run(changes)
        assert load_table_counts(dest_pl, "items") == {"items": 1}
    else:
        assert_load_info(dest_pl.run(changes))
        assert table_rows(dest_pl, "items", ["id", "a", "c"]) == [
            (1, "one", None),
            (2, None, "two"),
        ]


@pytest.mark.parametrize("backend", ["python", "pyarrow"])
@pytest.mark.parametrize("replica_identity", ["default", "full"])
def test_soft_delete(
    src_config: Tuple[dlt.Pipeline, str, str], replica_identity: str, backend: str
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, a text, b int);",
        f"ALTER TABLE {{schema}}.items REPLICA IDENTITY {replica_identity.upper()};",
        "INSERT INTO {schema}.items VALUES (1, 'one', 1), (2, 'two', 2);",
    )
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "items",
        initial_snapshot="direct",
        backend=backend,
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(snapshot))
    run_sql(
        src_pl,
        "DELETE FROM {schema}.items WHERE id = 1;",
        "UPDATE {schema}.items SET id = 20 WHERE id = 2;",
    )
    assert_load_info(
        dest_pl.run(
            replication_resource(
                slot_name, pub_name, hard_delete=False, backend=backend
            )
        )
    )
    rows = [
        (row[0], row[1], row[2], row[3] is not None)
        for row in table_rows(dest_pl, "items", ["id", "a", "b", "_dlt_deleted_ts"])
    ]
    if replica_identity == "full":
        # deletes keep the last values
        assert rows == [(1, "one", 1, True), (2, "two", 2, True), (20, "two", 2, False)]
    else:
        assert rows == [
            (1, None, None, True),
            (2, None, None, True),
            (20, "two", 2, False),
        ]


def test_legacy_columns(src_config: Tuple[dlt.Pipeline, str, str]) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(src_pl, "CREATE TABLE {schema}.items (id int PRIMARY KEY, a text);")
    init_replication(slot_name, pub_name, src_pl.dataset_name, "items")
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    # earlier versions of the source ordered changes by `lsn` and marked deletes in `deleted_ts`
    dest_pl.run(
        [{"id": 1, "a": "one", "lsn": 1, "deleted_ts": None}],
        table_name="items",
        write_disposition="merge",
        primary_key="id",
        columns={
            "lsn": {"data_type": "bigint", "dedup_sort": "desc"},
            "deleted_ts": {"data_type": "timestamp", "hard_delete": True},
        },
    )
    run_sql(
        src_pl,
        "INSERT INTO {schema}.items VALUES (1, 'one'), (2, 'two');",
        "DELETE FROM {schema}.items WHERE id = 1;",
        "UPDATE {schema}.items SET a = 'zwei' WHERE id = 2;",
    )
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    assert table_rows(dest_pl, "items", ["id", "a"]) == [(2, "zwei")]
    table = dest_pl.default_schema.get_table("items")
    assert get_dedup_sort_tuple(table) == ("_dlt_lsn", "desc")
    assert not table["columns"]["deleted_ts"].get("hard_delete")
    assert table["columns"]["_dlt_deleted_ts"]["hard_delete"] is True


@pytest.mark.parametrize(
    "initial_snapshot,backend",
    [
        ("table", "python"),
        ("unlogged_table", "pyarrow"),
        ("direct", "python"),
        ("direct", "pyarrow"),
    ],
)
def test_append_log_table(
    src_config: Tuple[dlt.Pipeline, str, str], initial_snapshot: str, backend: str
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.events (ts timestamptz, msg text);",
        "INSERT INTO {schema}.events VALUES ('2024-01-01 10:00+00', 'a'),"
        " ('2024-01-01 10:00+00', 'a'), ('2024-01-01 11:00+00', 'b');",
    )
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "events",
        publish="insert",
        initial_snapshot=initial_snapshot,
        backend=backend,
    )
    run_sql(
        src_pl,
        "INSERT INTO {schema}.events VALUES ('2024-01-01 10:00+00', 'a'),"
        " ('2024-01-01 12:00+00', 'c');",
        # not published, the loaded rows stay as they are
        "UPDATE {schema}.events SET msg = 'x' WHERE msg = 'b';",
        "DELETE FROM {schema}.events WHERE msg = 'c';",
    )

    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(snapshot))
    assert dest_pl.default_schema.get_table("events")["write_disposition"] == "append"
    assert_load_info(
        dest_pl.run(replication_resource(slot_name, pub_name, backend=backend))
    )
    table = dest_pl.default_schema.get_table("events")
    assert table["write_disposition"] == "append"
    assert not [name for name, c in table["columns"].items() if c.get("primary_key")]
    assert "_dlt_deleted_ts" not in table["columns"]
    assert "dedup_sort" not in table["columns"]["_dlt_lsn"]
    hour = [datetime(2024, 1, 1, h, tzinfo=timezone.utc) for h in range(10, 13)]
    assert table_rows(dest_pl, "events", ["ts", "msg"], order_by="ts, msg") == [
        (hour[0], "a"),
        (hour[0], "a"),
        (hour[0], "a"),
        (hour[1], "b"),
        (hour[2], "c"),
    ]


@pytest.mark.parametrize("initial_snapshot", ["table", "unlogged_table", "direct"])
def test_schema_publication_snapshot_not_replicated(
    src_config: Tuple[dlt.Pipeline, str, str], initial_snapshot: str
) -> None:
    if get_pg_version() < 150000:
        pytest.skip("incompatible Postgres server version")
    if not is_super_user(src_config[0].sql_client):
        pytest.skip("Postgres user needs to be superuser")
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.items (id int PRIMARY KEY, val text);",
        "INSERT INTO {schema}.items SELECT g, 'v' FROM generate_series(1, 1000) g;",
    )
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        initial_snapshot=initial_snapshot,
        publish="insert",
    )
    with src_pl.sql_client() as c:
        published = c.execute_sql(
            "SELECT tablename FROM pg_publication_tables WHERE pubname = %s;", pub_name
        )
    # a logged copy is published with the schema, its rows are skipped when decoding
    expected = 2 if initial_snapshot == "table" else 1
    assert len(published) == expected and "items" in [row[0] for row in published]
    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (1001, 'new');")

    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(snapshot))
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    assert load_table_counts(dest_pl, "items") == {"items": 1001}
    assert dest_pl.default_schema.data_table_names() == ["items"]


@pytest.mark.parametrize("initial_snapshot", ["table", "unlogged_table", "direct"])
def test_snapshot_isolated_from_concurrent_writes(
    src_config: Tuple[dlt.Pipeline, str, str],
    monkeypatch: pytest.MonkeyPatch,
    initial_snapshot: str,
) -> None:
    src_pl, slot_name, pub_name = src_config
    tables = ("a", "b")
    initial = [(i, 0) for i in range(1, 101)]
    create_tables(
        src_pl,
        *(
            f"CREATE TABLE {{schema}}.{table} (id int PRIMARY KEY, v int);"
            f" INSERT INTO {{schema}}.{table} SELECT g, 0 FROM generate_series(1, {len(initial)}) g;"
            for table in tables
        ),
    )
    dsn = dlt.secrets.get(
        "sources.pg_replication.credentials", ConnectionStringCredentials
    ).to_native_representation()
    schema = src_pl.dataset_name
    new_rows: List[Tuple[int, int]] = []

    def _read(table: str) -> List[Tuple[int, int]]:
        # a new connection sees all committed rows
        with closing(psycopg2.connect(dsn)) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT id, v FROM {schema}.{table} ORDER BY id")
            return cur.fetchall()

    def _insert_row(cur: Any, step: int) -> None:
        row = (len(initial) + step, step)
        for table in tables:
            cur.execute(f"INSERT INTO {schema}.{table} VALUES (%s, %s)", row)
        cur.connection.commit()
        new_rows.append(row)
        for table in tables:
            assert _read(table) == initial + new_rows

    barrier = threading.Barrier(2, timeout=30)
    errors: List[BaseException] = []

    def _write() -> None:
        try:
            with closing(psycopg2.connect(dsn)) as conn, conn.cursor() as cur:
                _insert_row(cur, 1)
                barrier.wait()  # sync 1: row 1 committed after the slot was created
                barrier.wait()  # sync 2: init_replication returned
                _insert_row(cur, 2)
                barrier.wait()  # sync 3: row 2 committed before the snapshot is read
        except threading.BrokenBarrierError:
            # the main thread failed and aborted the barrier
            pass
        except BaseException as ex:
            errors.append(ex)
            barrier.abort()

    writer = threading.Thread(target=_write, daemon=True)
    create_replication_slot = helpers.create_replication_slot

    def _create_slot_and_write(*args: Any, **kwargs: Any) -> Any:
        slot = create_replication_slot(*args, **kwargs)
        writer.start()
        barrier.wait()  # sync 1
        # table modes copy the tables after this
        return slot

    monkeypatch.setattr(helpers, "create_replication_slot", _create_slot_and_write)
    try:
        snapshot = init_replication(
            slot_name,
            pub_name,
            schema,
            tables,
            publish="insert",
            initial_snapshot=initial_snapshot,
        )
        barrier.wait()  # sync 2
        barrier.wait()  # sync 3
    except BaseException:
        barrier.abort()
        raise
    finally:
        if writer.ident is not None:
            writer.join(timeout=30)
        if errors:
            raise errors[0]

    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(snapshot))
    # the destination got the tables as they were when the slot was created
    for table in tables:
        assert table_rows(dest_pl, table, ["id", "v"]) == initial

    # appended, so a row in both the snapshot and the changes would be loaded twice
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    for table in tables:
        assert table_rows(dest_pl, table, ["id", "v"]) == initial + new_rows


def test_reset_slot_with_existing_state(
    src_config: Tuple[dlt.Pipeline, str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    src_pl, slot_name, pub_name = src_config
    warnings: List[str] = []
    monkeypatch.setattr(helpers.logger, "warning", warnings.append)
    create_tables(src_pl, "CREATE TABLE {schema}.items (id int PRIMARY KEY);")
    init_replication(slot_name, pub_name, src_pl.dataset_name, "items")
    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (1);")
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))

    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (2);")
    # the new slot starts after the LSN stored in the pipeline state
    snapshot = init_replication(
        slot_name,
        pub_name,
        src_pl.dataset_name,
        "items",
        persist_snapshots=True,
        reset=True,
    )
    assert any("`persist_snapshots` is deprecated" in w for w in warnings)
    assert_load_info(dest_pl.run(snapshot))
    run_sql(src_pl, "INSERT INTO {schema}.items VALUES (3);")
    assert_load_info(dest_pl.run(replication_resource(slot_name, pub_name)))
    assert table_rows(dest_pl, "items", ["id"]) == [(1,), (2,), (3,)]


def test_direct_snapshot_requires_sqlalchemy_2(
    src_config: Tuple[dlt.Pipeline, str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(src_pl, "CREATE TABLE {schema}.items (id int PRIMARY KEY);")
    monkeypatch.setattr(sqlalchemy, "__version__", "1.4.54")
    with pytest.raises(MissingDependencyException):
        init_replication(
            slot_name,
            pub_name,
            src_pl.dataset_name,
            "items",
            initial_snapshot="direct",
        )
    # fails before the slot is created
    with src_pl.sql_client() as c:
        assert not c.execute_sql(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s;", slot_name
        )


def _from_id_2(query: Any, table: Any) -> Any:
    return query.where(table.c.id >= 2)


def test_direct_snapshot_with_own_sql_tables(
    src_config: Tuple[dlt.Pipeline, str, str]
) -> None:
    src_pl, slot_name, pub_name = src_config
    create_tables(
        src_pl,
        "CREATE TABLE {schema}.a (id int PRIMARY KEY, v text);",
        "CREATE TABLE {schema}.b (id int PRIMARY KEY, v text);",
        "INSERT INTO {schema}.a VALUES (1, 'a1'), (2, 'a2'), (3, 'a3');"
        " INSERT INTO {schema}.b VALUES (1, 'b1'), (2, 'b2');",
    )
    credentials = dlt.secrets.get(
        "sources.pg_replication.credentials", ConnectionStringCredentials
    )
    dest_pl = dlt.pipeline(pipeline_name="dest_pl", destination="duckdb", dev_mode=True)
    with init_replication(
        slot_name, pub_name, src_pl.dataset_name, ("a", "b"), initial_snapshot="direct"
    ) as snapshot:
        # committed after the slot was created: replicated, not in the snapshot
        run_sql(
            src_pl,
            "INSERT INTO {schema}.a VALUES (4, 'a4');",
            "UPDATE {schema}.a SET v = 'a1 updated' WHERE id = 1;",
        )
        # every table is configured separately and reads the same snapshot
        table_a = sql_table(
            credentials,
            table="a",
            schema=src_pl.dataset_name,
            backend="pyarrow",
            query_adapter_callback=_from_id_2,
            engine_adapter_callback=snapshot.engine_adapter_callback,
            type_adapter_callback=snapshot.type_adapter_callback,
        )
        table_a.apply_hints(write_disposition="merge", primary_key="id")
        table_b = sql_table(
            credentials,
            table="b",
            schema=src_pl.dataset_name,
            backend="pyarrow",
            chunk_size=1,
            engine_adapter_callback=snapshot.engine_adapter_callback,
            type_adapter_callback=snapshot.type_adapter_callback,
        )
        assert_load_info(dest_pl.run([table_a, table_b]))
        assert table_rows(dest_pl, "a", ["id", "v"]) == [(2, "a2"), (3, "a3")]
        assert load_table_counts(dest_pl, "b") == {"b": 2}
    assert snapshot.closed

    assert_load_info(
        dest_pl.run(replication_resource(slot_name, pub_name, backend="pyarrow"))
    )
    # a row filtered out of the snapshot is loaded when it changes after the slot was created
    assert table_rows(dest_pl, "a", ["id", "v"]) == [
        (1, "a1 updated"),
        (2, "a2"),
        (3, "a3"),
        (4, "a4"),
    ]
