import pytest

from typing import Set, Tuple
from copy import deepcopy
from psycopg2.errors import InsufficientPrivilege

import dlt
from dlt.destinations.job_client_impl import SqlJobClientBase

from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    get_table_metrics,
)
from sources.pg_replication import replication_resource
from sources.pg_replication.helpers import (
    init_replication,
    get_pg_version,
)
from sources.pg_replication.exceptions import IncompatiblePostgresVersionException

from .cases import TABLE_ROW_ALL_DATA_TYPES, TABLE_UPDATE_COLUMNS_SCHEMA
from .utils import add_pk, assert_loaded_data, is_super_user


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
        table_names=None,  # will initialize schema replication and get list of snapshots from sql_database
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
    # because we track the whole schema
    @dlt.resource(write_disposition="append", primary_key="id_z")
    def tbl_z(data):
        yield data

    src_pl.run(tbl_z([{"id_z": 2, "val_x": "bar"}, {"id_z": 3}]))
    add_pk(src_pl.sql_client, "tbl_z", "id_z")
    # for some reason I need to add data to see that PK was added
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


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_core_functionality(
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
        schema_name=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y"),
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
def test_include_columns(
    src_config: Tuple[dlt.Pipeline, str, str], destination_name: str, init_load: bool
) -> None:
    def get_cols(pipeline: dlt.Pipeline, table_name: str) -> set:
        with pipeline.destination_client(pipeline.default_schema_name) as client:
            client: SqlJobClientBase
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
    )
    changes = replication_resource(
        slot_name=slot_name, pub_name=pub_name, include_columns=include_columns
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
    assert get_cols(dest_pl, "tbl_x") == {"id_x", "val_x", "lsn"}
    assert get_cols(dest_pl, "tbl_y") == {"id_y", "val_y", "lsn"}
    assert get_cols(dest_pl, "tbl_z") == {"id_z", "val_z", "another_col_z", "lsn"}


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
            InsufficientPrivilege if not is_su else IncompatiblePostgresVersionException
        )
        with pytest.raises(exp_err):
            init_replication(
                slot_name=slot_name,
                pub_name=pub_name,
                schema_name=src_pl.dataset_name,
            )


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
