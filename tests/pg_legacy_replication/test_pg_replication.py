from copy import deepcopy
from typing import Dict, Tuple

import dlt
import pytest
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.destinations.job_client_impl import SqlJobClientBase

from sources.pg_legacy_replication import (
    init_replication,
    cleanup_snapshot_resources,
    replication_source,
    ReplicationOptions,
)
from sources.pg_legacy_replication.helpers import SqlTableOptions, TableBackend
from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
)
from .cases import TABLE_ROW_ALL_DATA_TYPES, TABLE_UPDATE_COLUMNS_SCHEMA
from .utils import add_pk, assert_loaded_data

merge_hints: TTableSchemaColumns = {
    "_pg_deleted_ts": {"hard_delete": True},
    "_pg_lsn": {"dedup_sort": "desc"},
}


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_core_functionality(
    src_config: Tuple[dlt.Pipeline, str], destination_name: str, backend: TableBackend
) -> None:
    @dlt.resource(write_disposition="merge", primary_key="id_x")
    def tbl_x(data):
        yield data

    @dlt.resource(write_disposition="merge", primary_key="id_y")
    def tbl_y(data):
        yield data

    src_pl, slot_name = src_config

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
        schema=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y"),
        take_snapshots=True,
        table_options={
            "tbl_x": {"backend": backend},
            "tbl_y": {"backend": backend},
        },
    )

    changes = replication_source(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y"),
        repl_options={
            "tbl_x": {"backend": backend},
            "tbl_y": {"backend": backend},
        },
    )
    changes.tbl_x.apply_hints(
        write_disposition="merge", primary_key="id_x", columns=merge_hints
    )
    changes.tbl_y.apply_hints(
        write_disposition="merge", primary_key="id_y", columns=merge_hints
    )

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
    cleanup_snapshot_resources(snapshots)
    assert_load_info(info)
    assert load_table_counts(dest_pl, "tbl_x", "tbl_y") == {"tbl_x": 1, "tbl_y": 1}
    exp_tbl_x = [{"id_x": 1, "val_x": "foo"}]
    exp_tbl_y = [{"id_y": 1, "val_y": True}]
    assert_loaded_data(dest_pl, "tbl_x", ["id_x", "val_x"], exp_tbl_x, "id_x")
    assert_loaded_data(dest_pl, "tbl_y", ["id_y", "val_y"], exp_tbl_y, "id_y")

    # process changes
    info = dest_pl.run(changes)
    assert_load_info(info, expected_load_packages=2)
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
    assert_load_info(info, expected_load_packages=2)
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
    assert_load_info(info, expected_load_packages=2)
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
    assert_load_info(info, expected_load_packages=2)
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
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_without_init_load(
    src_config: Tuple[dlt.Pipeline, str], destination_name: str, backend: TableBackend
) -> None:
    @dlt.resource(write_disposition="merge", primary_key="id_x")
    def tbl_x(data):
        yield data

    @dlt.resource(write_disposition="merge", primary_key="id_y")
    def tbl_y(data):
        yield data

    src_pl, slot_name = src_config

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
        schema=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y"),
    )

    changes = replication_source(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y"),
        repl_options={
            "tbl_x": {"backend": backend},
            "tbl_y": {"backend": backend},
        },
    )
    changes.tbl_x.apply_hints(
        write_disposition="merge", primary_key="id_x", columns=merge_hints
    )
    changes.tbl_y.apply_hints(
        write_disposition="merge", primary_key="id_y", columns=merge_hints
    )

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


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("give_hints", [True, False])
@pytest.mark.parametrize("init_load", [True, False])
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_mapped_data_types(
    src_config: Tuple[dlt.Pipeline, str],
    destination_name: str,
    give_hints: bool,
    init_load: bool,
    backend: TableBackend,
) -> None:
    """Assert common data types (the ones mapped in PostgresTypeMapper) are properly handled."""

    data = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
    column_schema = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)

    # FIXME Need to figure out why when creating a snapshot my schema get loaded in another job
    expected_load_packages = 1
    if init_load:
        expected_load_packages = 2

    # resource to load data into postgres source table
    @dlt.resource(primary_key="col1", write_disposition="merge", columns=column_schema)
    def items(data):
        yield data

    src_pl, slot_name = src_config

    # create postgres table with single record containing all data types
    src_pl.run(items(data))
    add_pk(src_pl.sql_client, "items", "col1")

    # initialize replication and create resources
    snapshot = init_replication(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names="items",
        take_snapshots=init_load,
        table_options={"items": {"backend": backend}},
    )
    if init_load and give_hints:
        snapshot.items.apply_hints(columns=column_schema)

    changes = replication_source(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names="items",
        repl_options={"items": {"backend": backend}},
    )
    changes.items.apply_hints(
        write_disposition="merge", primary_key="col1", columns=merge_hints
    )
    if give_hints:
        changes.items.apply_hints(columns=column_schema)

    # initial load
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, dev_mode=True
    )
    if init_load:
        info = dest_pl.run(snapshot)
        cleanup_snapshot_resources(snapshot)
        assert_load_info(info)
        assert load_table_counts(dest_pl, "items")["items"] == 1

    # insert two records in postgres table
    r1 = deepcopy(data)
    r2 = deepcopy(data)
    r1["col1"] = 1
    r2["col1"] = 2
    src_pl.run(items([r1, r2]))

    info = dest_pl.run(changes)
    assert_load_info(info, expected_load_packages=expected_load_packages)
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
    assert_load_info(info, expected_load_packages=expected_load_packages)
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
    assert_load_info(info, expected_load_packages=expected_load_packages)
    assert load_table_counts(dest_pl, "items")["items"] == 3 if init_load else 2
    exp = [{"col1": 2, "col2": 2.5, "col3": False}]
    assert_loaded_data(
        dest_pl, "items", ["col1", "col2", "col3"], exp, "col1", "col1 = 2"
    )


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_unmapped_data_types(
    src_config: Tuple[dlt.Pipeline, str], destination_name: str, backend: TableBackend
) -> None:
    """Assert postgres data types that aren't explicitly mapped default to "text" type."""
    src_pl, slot_name = src_config

    # create postgres table with some unmapped types
    with src_pl.sql_client() as c:
        c.create_dataset()
        c.execute_sql(
            "CREATE TABLE data_types (bit_col bit(1), box_col box, uuid_col uuid);"
        )

    # initialize replication and create resource
    init_replication(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names="data_types",
    )
    changes = replication_source(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names="data_types",
        repl_options={"data_types": {"backend": backend}},
    )

    # insert record in source table to create replication item
    with src_pl.sql_client() as c:
        c.execute_sql(
            "INSERT INTO data_types VALUES (B'1', box '((1,1), (0,0))', gen_random_uuid());"
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


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("init_load", [True, False])
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_included_columns(
    src_config: Tuple[dlt.Pipeline, str],
    destination_name: str,
    init_load: bool,
    backend: TableBackend,
) -> None:
    def get_cols(pipeline: dlt.Pipeline, table_name: str) -> set:
        with pipeline.destination_client(pipeline.default_schema_name) as client:
            assert isinstance(client, SqlJobClientBase)
            return {
                k
                for k in client.get_storage_table(table_name)[1].keys()
                if not k.startswith("_dlt_")
            }

    @dlt.resource
    def tbl_x(data):
        yield data

    @dlt.resource
    def tbl_y(data):
        yield data

    @dlt.resource
    def tbl_z(data):
        yield data

    src_pl, slot_name = src_config

    # create three postgres tables
    src_pl.run(
        [
            tbl_x({"id_x": 1, "val_x": "foo", "another_col_x": 1}),
            tbl_y({"id_y": 1, "val_y": "foo", "another_col_y": 1}),
            tbl_z({"id_z": 1, "val_z": "foo", "another_col_z": 1}),
        ]
    )

    # initialize replication and create resources
    table_options = {
        "tbl_x": {"backend": backend, "included_columns": {"id_x", "val_x"}},
        "tbl_y": {"backend": backend, "included_columns": {"id_y", "val_y"}},
        "tbl_z": {"backend": backend},
        # tbl_z is not specified, hence all columns should be included
    }
    snapshots = init_replication(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y", "tbl_z"),
        take_snapshots=init_load,
        table_options=table_options,
    )
    changes = replication_source(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y", "tbl_z"),
        repl_options=table_options,
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
        cleanup_snapshot_resources(snapshots)
        assert get_cols(dest_pl, "tbl_x") == {"id_x", "val_x"}
        assert get_cols(dest_pl, "tbl_y") == {"id_y", "val_y"}
        assert get_cols(dest_pl, "tbl_z") == {"id_z", "val_z", "another_col_z"}

    dest_pl.run(changes)
    assert get_cols(dest_pl, "tbl_x") == {"id_x", "val_x", "_pg_lsn", "_pg_deleted_ts"}
    assert get_cols(dest_pl, "tbl_y") == {"id_y", "val_y", "_pg_lsn", "_pg_deleted_ts"}
    assert get_cols(dest_pl, "tbl_z") == {
        "id_z",
        "val_z",
        "another_col_z",
        "_pg_lsn",
        "_pg_deleted_ts",
    }


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("init_load", [True, False])
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_column_hints(
    src_config: Tuple[dlt.Pipeline, str],
    destination_name: str,
    init_load: bool,
    backend: TableBackend,
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

    src_pl, slot_name = src_config

    # create three postgres tables
    src_pl.run(
        [
            tbl_x({"id_x": 1, "val_x": "foo", "another_col_x": 1}),
            tbl_y({"id_y": 1, "val_y": "foo", "another_col_y": 1}),
            tbl_z({"id_z": 1, "val_z": "foo", "another_col_z": 1}),
        ]
    )

    # initialize replication and create resources
    snapshots = init_replication(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y", "tbl_z"),
        take_snapshots=init_load,
        table_options={
            "tbl_x": {"backend": backend},
            "tbl_y": {"backend": backend},
            "tbl_z": {"backend": backend},
        },
    )
    if init_load:
        snapshots.tbl_x.apply_hints(columns={"another_col_x": {"data_type": "double"}})
        snapshots.tbl_y.apply_hints(columns={"another_col_y": {"precision": 32}})

    changes = replication_source(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y", "tbl_z"),
        repl_options={
            "tbl_x": {
                "backend": backend,
                "column_hints": {"another_col_x": {"data_type": "double"}},
            },
            "tbl_y": {
                "backend": backend,
                "column_hints": {"another_col_y": {"precision": 32}},
            },
            "tbl_z": {"backend": backend},
        },
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
        cleanup_snapshot_resources(snapshots)
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
@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_table_schema_change(
    src_config: Tuple[dlt.Pipeline, str], destination_name: str, backend: TableBackend
) -> None:
    src_pl, slot_name = src_config

    # create postgres table
    src_pl.run([{"c1": 1, "c2": 1}], table_name="items")

    # initialize replication
    init_replication(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names="items",
    )

    # create resource and pipeline
    changes = replication_source(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names="items",
        repl_options={"items": {"backend": backend}},
    )
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


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_batching(src_config: Tuple[dlt.Pipeline, str], backend: TableBackend) -> None:
    # this test asserts the number of data items yielded by the replication resource
    # is not affected by `target_batch_size` and the number of replication messages per transaction
    src_pl, slot_name = src_config

    # create postgres table with single record
    data = {"id": 1000, "val": True}
    src_pl.run([data], table_name="items")

    # initialize replication and create resource for changes
    init_replication(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names="items",
    )
    changes = replication_source(
        slot_name=slot_name,
        schema=src_pl.dataset_name,
        table_names="items",
        target_batch_size=50,
        repl_options={"items": {"backend": backend}},
    )

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
