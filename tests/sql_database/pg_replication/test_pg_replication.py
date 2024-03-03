import pytest

from typing import Iterator
from copy import deepcopy

import dlt

from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    load_table_counts,
    select_data,
)
from sources.sql_database.pg_replication import table_changes
from sources.sql_database.pg_replication.helpers import (
    init_table_replication,
    _gen_table_replication_references,
)

from .cases import TABLE_ROW_ALL_DATA_TYPES, TABLE_UPDATE_COLUMNS_SCHEMA


TABLE_NAME = "items"


@pytest.fixture()
def src_pl() -> Iterator[dlt.Pipeline]:
    # setup
    src_pl = dlt.pipeline(
        destination="postgres", dataset_name="src_pl", full_refresh=True
    )
    yield src_pl
    # teardown
    with src_pl.sql_client() as c:
        c.drop_dataset()
        slot_name, publication_name = _gen_table_replication_references(
            TABLE_NAME, src_pl.dataset_name
        )
        c.execute_sql(f"SELECT pg_drop_replication_slot('{slot_name}');")
        c.execute_sql(f"DROP PUBLICATION IF EXISTS {publication_name};")
        with c.with_staging_dataset(staging=True):
            c.drop_dataset()


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_all_data_types(src_pl: dlt.Pipeline, destination_name: str):
    # resource to load data into postgres source table
    @dlt.resource(
        name=TABLE_NAME,
        primary_key="col1",  # will not be physically applied on Postgres table
        write_disposition="merge",
        columns=TABLE_UPDATE_COLUMNS_SCHEMA,
    )
    def items(data):
        yield data

    # create postgres table with single record containing all data types
    data = TABLE_ROW_ALL_DATA_TYPES
    src_pl.run(items(data))

    # add primary key that serves as REPLICA IDENTITY, necessary when publishing UPDATEs and/or DELETEs
    with src_pl.sql_client() as c:
        qual_name = c.make_qualified_table_name(TABLE_NAME)
        c.execute_sql(f"ALTER TABLE {qual_name} ADD PRIMARY KEY (col1);")

    # excludes dlt system columns from replication
    include_columns = data.keys()

    # initialize table replication, persist snapshot for initial load
    slot_name, publication_name, table_snapshot = init_table_replication(
        table=TABLE_NAME,
        schema=src_pl.dataset_name,
        persist_snapshot=True,
        include_columns=include_columns,
    )
    table_snapshot.apply_hints(
        columns=TABLE_UPDATE_COLUMNS_SCHEMA
    )  # TODO: automatically get column schema from source table?

    # initial load
    dest_pl = dlt.pipeline(
        destination=destination_name, dataset_name="dest_pl", full_refresh=True
    )
    info = dest_pl.run(table_snapshot)
    assert_load_info(info)
    assert load_table_counts(dest_pl, TABLE_NAME)[TABLE_NAME] == 1

    # insert two records in postgres table
    r1 = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
    r2 = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
    r1["col1"] = 1
    r2["col1"] = 2
    src_pl.run(items([r1, r2]))

    r = table_changes(
        table=TABLE_NAME,
        primary_key="col1",
        include_columns=include_columns,
        slot_name=slot_name,
        publication_name=publication_name,
    )
    info = dest_pl.run(r)
    assert_load_info(info)
    assert load_table_counts(dest_pl, TABLE_NAME)[TABLE_NAME] == 3

    # compare observed with expected column types
    observed = dest_pl.default_schema.get_table("items")["columns"]
    for name, expected in TABLE_UPDATE_COLUMNS_SCHEMA.items():
        assert observed[name]["data_type"] == expected["data_type"]
        # postgres bytea does not have precision
        if expected.get("precision") is not None and expected["data_type"] != "binary":
            assert observed[name]["precision"] == expected["precision"]

    # update two records in postgres table
    # this does two deletes and two inserts because dlt implements "merge" and "delete-and-insert"
    # as such, postgres will create four replication messages: two of type Delete and two of type Insert
    r1["col2"] = 1.5
    r2["col3"] = False
    src_pl.run(items([r1, r2]))

    info = dest_pl.run(r)
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
    info = dest_pl.run(r)
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
