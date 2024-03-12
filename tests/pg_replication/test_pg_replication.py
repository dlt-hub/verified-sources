import pytest

from copy import deepcopy

import dlt
from dlt.extract.resource import DltResource

from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    select_data,
)
from sources.pg_replication import (
    table_changes,
    replicated_table,
    pg_replication_source,
)
from sources.pg_replication.helpers import (
    init_table_replication,
    _gen_table_replication_references,
)

from .cases import TABLE_ROW_ALL_DATA_TYPES, TABLE_UPDATE_COLUMNS_SCHEMA
from .conftest import TABLE_NAME
from .utils import add_pk


@pytest.mark.parametrize("persist_snapshot", [True, False])
def test_init_table_replication(src_pl: dlt.Pipeline, persist_snapshot: bool):
    # resource to load data into postgres source table
    @dlt.resource(table_name=TABLE_NAME, primary_key="id", write_disposition="merge")
    def items():
        yield {"id": 1, "val": True}

    # create postgres table with single record
    src_pl.run(items())

    # initialize table replication for table_x, persist snapshot for initial load
    slot_name, pub_name, table_snapshot = init_table_replication(
        table_name=TABLE_NAME,
        schema_name=src_pl.dataset_name,
        persist_snapshot=persist_snapshot,
    )
    expected_slot_name, expected_pub_name = _gen_table_replication_references(
        TABLE_NAME, src_pl.dataset_name
    )
    assert slot_name == expected_slot_name
    assert pub_name == expected_pub_name
    if persist_snapshot:
        assert isinstance(table_snapshot, DltResource)
    else:
        assert table_snapshot is None

    # initialize table replication again
    # method should return names of existing replication slot and publication name
    # `table_snapshot` should be None—also when `persist_snapshot` is True, because
    # a snapshot is only created when the slot is created
    slot_name, pub_name, table_snapshot = init_table_replication(
        table_name=TABLE_NAME,
        schema_name=src_pl.dataset_name,
        persist_snapshot=persist_snapshot,
    )
    assert slot_name == expected_slot_name
    assert pub_name == expected_pub_name
    assert table_snapshot is None

    # initialize table replication again, now use `reset` arg to drop and
    # recreate the slot and publication
    # since there is a new slot, a `table_snapshot` should be returned when
    # `persist_snapshot` is True
    slot_name, pub_name, table_snapshot = init_table_replication(
        table_name=TABLE_NAME,
        schema_name=src_pl.dataset_name,
        persist_snapshot=persist_snapshot,
        reset=True,
    )
    expected_slot_name, expected_pub_name = _gen_table_replication_references(
        TABLE_NAME, src_pl.dataset_name
    )
    assert slot_name == expected_slot_name
    assert pub_name == expected_pub_name
    if persist_snapshot:
        assert isinstance(table_snapshot, DltResource)
    else:
        assert table_snapshot is None


@pytest.mark.parametrize("publish", ["insert", "insert, update, delete"])
def test_write_disposition(src_pl: dlt.Pipeline, publish: str):
    # resource to load data into postgres source table
    @dlt.resource(name=TABLE_NAME, primary_key="id", write_disposition="merge")
    def items(data):
        yield data

    # create postgres table with single record
    src_pl.run(items({"id": 1, "val": True}))

    if publish == "insert, update, delete":
        add_pk(src_pl.sql_client, TABLE_NAME, "id")

    # initialize replication, create resources for snapshot and changes
    _, _, table_snapshot = init_table_replication(
        table_name=TABLE_NAME,
        schema_name=src_pl.dataset_name,
        publish=publish,
        persist_snapshot=True,
    )
    changes = table_changes(table_name=TABLE_NAME, schema_name=src_pl.dataset_name)

    # assert write dispositions
    expected_write_disposition = "append" if publish == "insert" else "merge"
    assert table_snapshot.write_disposition == expected_write_disposition
    assert changes.write_disposition == expected_write_disposition

    # also check replicated_table resource
    rep_tbl = replicated_table(
        table_name=TABLE_NAME,
        schema_name=src_pl.dataset_name,
        init_conf={"publish": publish, "persist_snapshot": True, "reset": True},
    )
    assert rep_tbl.write_disposition == expected_write_disposition


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("give_hints", [True, False])
@pytest.mark.parametrize("explicit_init", [True, False])
def test_all_data_types(
    src_pl: dlt.Pipeline,
    destination_name: str,
    give_hints: bool,
    explicit_init: bool,
):
    data = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
    column_schema = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)

    # resource to load data into postgres source table
    @dlt.resource(
        name=TABLE_NAME,
        primary_key="col1",
        write_disposition="merge",
        columns=column_schema,
    )
    def items(data):
        yield data

    # create postgres table with single record containing all data types
    src_pl.run(items(data))
    add_pk(src_pl.sql_client, TABLE_NAME, "col1")

    # excludes dlt system columns from replication
    include_columns = data.keys()

    if explicit_init:
        slot_name, pub_name, table_snapshot = init_table_replication(
            table_name=TABLE_NAME,
            schema_name=src_pl.dataset_name,
            columns=column_schema if give_hints else None,
            include_columns=include_columns,
            persist_snapshot=True,
        )
    else:
        # init will be done inside `replicated_table` function
        slot_name = None
        pub_name = None
        table_snapshot = None

    rep_tbl = replicated_table(
        table_name=TABLE_NAME,
        schema_name=src_pl.dataset_name,
        columns=column_schema if give_hints else None,
        include_columns=include_columns,
        init_conf={"persist_snapshot": True},
        slot_name=slot_name,
        pub_name=pub_name,
        table_snapshot=table_snapshot,
    )

    # initial load
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, full_refresh=True
    )
    info = dest_pl.run(rep_tbl)
    assert_load_info(info)
    assert load_table_counts(dest_pl, TABLE_NAME)[TABLE_NAME] == 1

    # insert two records in postgres table
    r1 = deepcopy(data)
    r2 = deepcopy(data)
    r1["col1"] = 1
    r2["col1"] = 2
    src_pl.run(items([r1, r2]))

    info = dest_pl.run(rep_tbl)
    assert_load_info(info)
    assert load_table_counts(dest_pl, TABLE_NAME)[TABLE_NAME] == 3

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

    info = dest_pl.run(rep_tbl)
    assert_load_info(info)
    assert load_table_counts(dest_pl, TABLE_NAME)[TABLE_NAME] == 3

    # compare observed records with expected records
    qual_name = dest_pl.sql_client().make_qualified_table_name(TABLE_NAME)
    observed = [
        {"col1": row[0], "col2": row[1], "col3": row[2]}
        for row in select_data(dest_pl, f"SELECT col1, col2, col3 FROM {qual_name}")
    ]
    expected = [
        {"col1": 1, "col2": 1.5, "col3": True},
        {"col1": 2, "col2": 898912.821982, "col3": False},
        {"col1": 989127831, "col2": 898912.821982, "col3": True},
    ]
    assert sorted(observed, key=lambda d: d["col1"]) == expected

    # now do an actual update, so postgres will create a replication message of type Update
    with src_pl.sql_client() as c:
        qual_name = src_pl.sql_client().make_qualified_table_name(TABLE_NAME)
        c.execute_sql(f"UPDATE {qual_name} SET col2 = 2.5 WHERE col1 = 989127831;")

    # load the change to the destination
    info = dest_pl.run(rep_tbl)
    assert_load_info(info)
    assert load_table_counts(dest_pl, TABLE_NAME)[TABLE_NAME] == 3

    # compare observed records with expected records
    qual_name = dest_pl.sql_client().make_qualified_table_name(TABLE_NAME)
    observed = [
        {"col1": row[0], "col2": row[1], "col3": row[2]}
        for row in select_data(
            dest_pl, f"SELECT col1, col2, col3 FROM {qual_name} WHERE col1 = 989127831;"
        )
    ]
    expected = [{"col1": 989127831, "col2": 2.5, "col3": True}]
    assert sorted(observed, key=lambda d: d["col1"]) == expected


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_insert_only(src_pl: dlt.Pipeline, destination_name: str):
    # resource to load data into postgres source table
    @dlt.resource(name=TABLE_NAME, write_disposition="append")
    def items(data):
        yield data

    # create postgres table with single record
    data = {"id": 1, "foo": "bar"}
    src_pl.run(items(data))

    # excludes dlt system columns from replication
    include_columns = data.keys()

    # initialize table replication, persist snapshot for initial load
    slot_name, pub_name, table_snapshot = init_table_replication(
        table_name=TABLE_NAME,
        schema_name=src_pl.dataset_name,
        publish="insert",
        persist_snapshot=True,
        include_columns=include_columns,
    )

    # initial load
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, full_refresh=True
    )
    extract_info = dest_pl.extract(table_snapshot)
    assert extract_info.asdict()["job_metrics"][0]["items_count"] == 1

    # insert a record in postgres table
    data = {"id": 2, "foo": "bar"}
    src_pl.run(items(data))

    # create resource for table changes
    changes = table_changes(
        table_name=TABLE_NAME,
        schema_name=src_pl.dataset_name,
        include_columns=include_columns,
        slot_name=slot_name,
        pub_name=pub_name,
    )

    # extract items from resource
    extract_info = dest_pl.extract(changes)
    assert extract_info.asdict()["job_metrics"][0]["items_count"] == 1

    # do an update and a delete—these operations should not lead to items in the resource
    with src_pl.sql_client() as c:
        qual_name = src_pl.sql_client().make_qualified_table_name(TABLE_NAME)
        c.execute_sql(f"UPDATE {qual_name} SET foo = 'baz' WHERE id = 2;")
        c.execute_sql(f"DELETE FROM {qual_name} WHERE id = 2;")
    extract_info = dest_pl.extract(changes)
    assert extract_info.asdict()["job_metrics"] == []


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_batching(src_pl: dlt.Pipeline, destination_name: str):
    # this test asserts the number of data items yielded by the `table_changes`
    # resource is not affected by `target_batch_size` and the number of replication
    # messages per transaction

    # resource to load data into postgres source table
    @dlt.resource(name=TABLE_NAME, primary_key="id", write_disposition="merge")
    def items(data):
        yield data

    # create postgres table with single record
    data = {"id": 1000, "val": True}
    src_pl.run(items(data))
    add_pk(src_pl.sql_client, TABLE_NAME, "id")

    # excludes dlt system columns from replication
    include_columns = data.keys()

    # initialize table replication
    slot_name, pub_name, _ = init_table_replication(
        table_name=TABLE_NAME,
        schema_name=src_pl.dataset_name,
        persist_snapshot=False,
        include_columns=include_columns,
    )

    # create destination pipeline and resource
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, full_refresh=True
    )
    changes = table_changes(
        table_name=TABLE_NAME,
        schema_name=src_pl.dataset_name,
        include_columns=include_columns,
        target_batch_size=50,
        slot_name=slot_name,
        pub_name=pub_name,
    )

    # insert 100 records into source table in one transaction
    batch = [{**r, **{"id": key}} for r in [data] for key in range(1, 101)]
    src_pl.run(items(batch))
    extract_info = dest_pl.extract(changes)
    assert extract_info.asdict()["job_metrics"][0]["items_count"] == 100

    # insert 100 records into source table in 5 transactions
    batch = [{**r, **{"id": key}} for r in [data] for key in range(101, 121)]
    src_pl.run(items(batch))
    batch = [{**r, **{"id": key}} for r in [data] for key in range(121, 141)]
    src_pl.run(items(batch))
    batch = [{**r, **{"id": key}} for r in [data] for key in range(141, 161)]
    src_pl.run(items(batch))
    batch = [{**r, **{"id": key}} for r in [data] for key in range(161, 181)]
    src_pl.run(items(batch))
    batch = [{**r, **{"id": key}} for r in [data] for key in range(181, 201)]
    src_pl.run(items(batch))
    extract_info = dest_pl.extract(changes)
    assert extract_info.asdict()["job_metrics"][0]["items_count"] == 100


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("give_conf", [True, False])
def test_source_multiple_tables(
    src_pl: dlt.Pipeline, destination_name: str, give_conf: bool
):
    # resources to load data into postgres source tables
    @dlt.resource(primary_key="id_w", write_disposition="merge")
    def items_w(data):
        yield data

    @dlt.resource(primary_key="id_x", write_disposition="merge")
    def items_x(data):
        yield data

    @dlt.resource(primary_key="id_y", write_disposition="merge")
    def items_y(data):
        yield data

    @dlt.resource(primary_key="id_z", write_disposition="merge")
    def items_z(data):
        yield data

    # create two postgres tables with single record
    resources = [
        items_w({"id_w": 1, "val": [1, 2, 3]}),
        items_x({"id_x": 1, "val": "foo"}),
        items_y([{"id_y": 1, "val": True}, {"id_y": 2, "val": False}]),
        items_z({"id_z": 1, "val": "2024-03-11"}),
    ]
    src_pl.run(resources)
    add_pk(src_pl.sql_client, "items_w", "id_w")
    add_pk(src_pl.sql_client, "items_x", "id_x")
    add_pk(src_pl.sql_client, "items_y", "id_y")
    if not give_conf:
        add_pk(src_pl.sql_client, "items_z", "id_z")

    conf = (
        {
            "items_x": {
                "include_columns": ["id_x", "val"],
                "init_conf": {"persist_snapshot": True},
            },
            "items_y": {
                "include_columns": ["id_y", "val"],
                "init_conf": {"persist_snapshot": False},
            },
            "items_z": {
                "include_columns": ["id_z", "val"],
                "init_conf": {"persist_snapshot": True, "publish": "insert"},
            },
        }
        if give_conf
        else None
    )

    rep_tbls = pg_replication_source(
        table_names=["items_w", "items_x", "items_y", "items_z"],
        schema_name=src_pl.dataset_name,
        conf=conf,
    )
    dest_pl = dlt.pipeline(
        pipeline_name="dest_pl", destination=destination_name, full_refresh=True
    )
    info = dest_pl.run(rep_tbls)
    assert_load_info(info)
    if give_conf:
        assert load_table_counts(dest_pl, "items_x", "items_z") == {
            "items_x": 1,
            "items_z": 1,
        }
        with pytest.raises(dlt.destinations.exceptions.DatabaseUndefinedRelation):
            # "items_w" table does not exist because we didn't specify "persist_snapshot" and it defaults to False
            load_table_counts(dest_pl, "items_w")
        with pytest.raises(dlt.destinations.exceptions.DatabaseUndefinedRelation):
            # "items_y" table does not exist because we set "persist_snapshot" to False
            load_table_counts(dest_pl, "items_y")

    # insert one record in both postgres tables
    resources = [
        items_w({"id_w": 2, "val": [1, 2]}),
        items_x({"id_x": 2, "val": "foo"}),
        items_y({"id_y": 3, "val": True}),
        items_z({"id_z": 2, "val": "2000-01-01"}),
    ]
    src_pl.run(resources)

    info = dest_pl.run(rep_tbls)
    assert_load_info(info)
    if give_conf:
        assert load_table_counts(
            dest_pl, "items_w", "items_x", "items_y", "items_z"
        ) == {
            "items_w": 1,
            "items_x": 2,
            "items_y": 1,
            "items_z": 2,
        }
    else:
        assert load_table_counts(
            dest_pl, "items_w", "items_x", "items_y", "items_z"
        ) == {
            "items_w": 1,
            "items_x": 1,
            "items_y": 1,
            "items_z": 1,
        }
