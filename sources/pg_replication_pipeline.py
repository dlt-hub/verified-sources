from typing import Optional, Union, Sequence
import dlt

from dlt.common.destination import Destination
from dlt.destinations.impl.postgres.configuration import PostgresCredentials

from pg_replication import replication_resource
from pg_replication.helpers import init_replication


PG_CREDS = dlt.secrets.get("sources.pg_replication.credentials", PostgresCredentials)


def replicate_single_table_with_initial_load(
    schema_name: str, table_names: Optional[Union[str, Sequence[str]]]
) -> None:
    """Sets up replication with initial load for your existing PostgreSQL database.

    Unlike the other functions, this function does NOT simulate changes in the source table.
    It connects to your actual database and performs initial load from the specified tables
    and performs replication if any.

    Args:
        schema_name (str): Name of the schema containing the tables to replicate.
        table_names (Optional[Union[str, Sequence[str]]]): Name(s) of the table(s)
            to replicate. Can be a single table name as string or a sequence of table names.
            If None, replicates all tables in the schema (requires superuser privileges).
            When specifying table names, the Postgres user must own the tables or be a superuser.

    Returns:
        None
    """
    # create destination pipeline
    dest_pl = dlt.pipeline(
        pipeline_name="pg_replication_pipeline",
        destination="duckdb",
        dataset_name="replicate_with_initial_load",
    )

    # initialize replication for the source table
    slot_name = "example_slot"
    pub_name = "example_pub"
    snapshot = init_replication(  # requires the Postgres user to have the REPLICATION attribute assigned
        slot_name=slot_name,
        pub_name=pub_name,
        table_names=table_names,  # requires the Postgres user to own the table(s) or be superuser
        schema_name=schema_name,
        persist_snapshots=True,  # persist snapshot table(s) and let function return resource(s) for initial load
        reset=True,
    )

    # perform initial load to capture all records present in source table prior to replication initialization
    load_info = dest_pl.run(snapshot)
    print(load_info)
    print(dest_pl.last_trace.last_normalize_info)

    # assuming there were changes in the source table, propagate change to destination
    changes = replication_resource(slot_name, pub_name)
    load_info = dest_pl.run(changes)
    print(load_info)
    print(dest_pl.last_trace.last_normalize_info)


def replicate_single_table_demo() -> None:
    """Demonstrates PostgreSQL replication by simulating a source table and changes.

    Shows basic usage of `init_replication` helper and `replication_resource` resource.
    This demo creates a source table and simulates INSERT, UPDATE, and DELETE operations
    to show how replication works end-to-end. In production, you would have an existing
    PostgreSQL database with real changes instead of simulating them.
    """
    # create source and destination pipelines
    src_pl = get_postgres_pipeline()
    dest_pl = dlt.pipeline(
        pipeline_name="pg_replication_pipeline",
        destination="duckdb",
        dataset_name="replicate_single_table",
        dev_mode=True,
    )

    # create table "my_source_table" in source to demonstrate replication
    create_source_table(
        src_pl, "CREATE TABLE {table_name} (id integer PRIMARY KEY, val bool);"
    )

    # initialize replication for the source table—this creates a replication slot and publication
    slot_name = "example_slot"
    pub_name = "example_pub"
    init_replication(  # requires the Postgres user to have the REPLICATION attribute assigned
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="my_source_table",
        reset=True,
    )

    # create a resource that generates items for each change in the source table
    changes = replication_resource(slot_name, pub_name)

    # insert two records in source table and propagate changes to destination
    change_source_table(
        src_pl, "INSERT INTO {table_name} VALUES (1, true), (2, false);"
    )
    dest_pl.run(changes)
    show_destination_table(dest_pl)

    # update record in source table and propagate change to destination
    change_source_table(src_pl, "UPDATE {table_name} SET val = true WHERE id = 2;")
    dest_pl.run(changes)
    show_destination_table(dest_pl)

    # delete record from source table and propagate change to destination
    change_source_table(src_pl, "DELETE FROM {table_name} WHERE id = 2;")
    dest_pl.run(changes)
    show_destination_table(dest_pl)


def replicate_with_initial_load_demo() -> None:
    """Demonstrates PostgreSQL replication with initial load by simulating a source table and changes.

    Shows usage of `persist_snapshots` argument and snapshot resource returned by `init_replication` helper.
    This demo creates a source table with existing data, then simulates additional changes to show how
    initial load captures pre-existing records and replication handles subsequent changes.
    """
    # create source and destination pipelines
    src_pl = get_postgres_pipeline()
    dest_pl = dlt.pipeline(
        pipeline_name="pg_replication_pipeline",
        destination="duckdb",
        dataset_name="replicate_with_initial_load",
        dev_mode=True,
    )

    # create table "my_source_table" in source to demonstrate replication
    create_source_table(
        src_pl, "CREATE TABLE {table_name} (id integer PRIMARY KEY, val bool);"
    )

    # insert records before initializing replication
    change_source_table(
        src_pl, "INSERT INTO {table_name} VALUES (1, true), (2, false);"
    )

    # initialize replication for the source table
    slot_name = "example_slot"
    pub_name = "example_pub"
    snapshot = init_replication(  # requires the Postgres user to have the REPLICATION attribute assigned
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="my_source_table",
        persist_snapshots=True,  # persist snapshot table(s) and let function return resource(s) for initial load
        reset=True,
    )

    # perform initial load to capture all records present in source table prior to replication initialization
    dest_pl.run(snapshot)
    show_destination_table(dest_pl)

    # insert record in source table and propagate change to destination
    change_source_table(src_pl, "INSERT INTO {table_name} VALUES (3, true);")
    changes = replication_resource(slot_name, pub_name)
    dest_pl.run(changes)
    show_destination_table(dest_pl)


def replicate_entire_schema_demo() -> None:
    """Demonstrates schema-level replication by simulating multiple tables and changes.

    Shows setup and usage of schema replication, which captures changes across all tables
    in a schema. This demo creates multiple source tables and simulates changes to show
    how schema replication works, including tables added after replication starts.

    Schema replication requires PostgreSQL server version 15 or higher. An exception
    is raised if that's not the case.
    """
    # create source and destination pipelines
    src_pl = get_postgres_pipeline()
    dest_pl = dlt.pipeline(
        pipeline_name="pg_replication_pipeline",
        destination="duckdb",
        dataset_name="replicate_entire_schema",
        dev_mode=True,
    )

    # create two source tables to demonstrate schema replication
    create_source_table(
        src_pl,
        "CREATE TABLE {table_name} (id integer PRIMARY KEY, val bool);",
        "tbl_x",
    )
    create_source_table(
        src_pl,
        "CREATE TABLE {table_name} (id integer PRIMARY KEY, val varchar);",
        "tbl_y",
    )

    # initialize schema replication by omitting the `table_names` argument
    slot_name = "example_slot"
    pub_name = "example_pub"
    init_replication(  # initializing schema replication requires the Postgres user to be a superuser
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        reset=True,
    )

    # create a resource that generates items for each change in the schema's tables
    changes = replication_resource(slot_name, pub_name)

    # insert records in source tables and propagate changes to destination
    change_source_table(
        src_pl, "INSERT INTO {table_name} VALUES (1, true), (2, false);", "tbl_x"
    )
    change_source_table(src_pl, "INSERT INTO {table_name} VALUES (1, 'foo');", "tbl_y")
    dest_pl.run(changes)
    show_destination_table(dest_pl, "tbl_x")
    show_destination_table(dest_pl, "tbl_y")

    # tables added to the schema later are also included in the replication
    create_source_table(
        src_pl, "CREATE TABLE {table_name} (id integer PRIMARY KEY, val date);", "tbl_z"
    )
    change_source_table(
        src_pl, "INSERT INTO {table_name} VALUES (1, '2023-03-18');", "tbl_z"
    )
    dest_pl.run(changes)
    show_destination_table(dest_pl, "tbl_z")


def replicate_with_column_selection_demo() -> None:
    """Demonstrates column selection in replication by simulating tables with selective column capture.

    Shows usage of `include_columns` argument to replicate only specific columns from tables.
    This demo creates source tables and simulates changes to show how column selection works,
    where some tables have filtered columns while others include all columns by default.
    """
    # create source and destination pipelines
    src_pl = get_postgres_pipeline()
    dest_pl = dlt.pipeline(
        pipeline_name="pg_replication_pipeline",
        destination="duckdb",
        dataset_name="replicate_with_column_selection",
        dev_mode=True,
    )

    # create two source tables to demonstrate schema replication
    create_source_table(
        src_pl,
        "CREATE TABLE {table_name} (c1 integer PRIMARY KEY, c2 bool, c3 varchar);",
        "tbl_x",
    )
    create_source_table(
        src_pl,
        "CREATE TABLE {table_name} (c1 integer PRIMARY KEY, c2 bool, c3 varchar);",
        "tbl_y",
    )

    # initialize schema replication by omitting the `table_names` argument
    slot_name = "example_slot"
    pub_name = "example_pub"
    init_replication(  # requires the Postgres user to have the REPLICATION attribute assigned
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names=("tbl_x", "tbl_y"),
        reset=True,
    )

    # create a resource that generates items for each change in the schema's tables
    changes = replication_resource(
        slot_name=slot_name,
        pub_name=pub_name,
        include_columns={
            "tbl_x": ("c1", "c2")
        },  # columns not specified here are excluded from generated data items
    )

    # insert records in source tables and propagate changes to destination
    change_source_table(
        src_pl, "INSERT INTO {table_name} VALUES (1, true, 'foo');", "tbl_x"
    )
    change_source_table(
        src_pl, "INSERT INTO {table_name} VALUES (1, false, 'bar');", "tbl_y"
    )
    dest_pl.run(changes)

    # show columns in schema for both tables
    # column c3 is not in the schema for tbl_x because we did not include it
    # tbl_y does have column c3 because we didn't specify include columns for this table and by default all columns are included
    print("tbl_x", ":", list(dest_pl.default_schema.get_table_columns("tbl_x").keys()))
    print("tbl_y", ":", list(dest_pl.default_schema.get_table_columns("tbl_y").keys()))


# define some helper methods to make examples more readable


def get_postgres_pipeline() -> dlt.Pipeline:
    """Returns a pipeline loading into `postgres` destination.

    Uses workaround to fix destination to `postgres`, so it does not get replaced
    during `dlt init`.
    """
    # this trick prevents dlt init command from replacing "destination" argument to "pipeline"
    p_call = dlt.pipeline
    pipe = p_call(
        pipeline_name="source_pipeline",
        destination=Destination.from_reference("postgres", credentials=PG_CREDS),
        dataset_name="source_dataset",
        dev_mode=True,
    )
    return pipe


def create_source_table(
    src_pl: dlt.Pipeline, sql: str, table_name: str = "my_source_table"
) -> None:
    with src_pl.sql_client() as c:
        try:
            c.create_dataset()
        except dlt.destinations.exceptions.DatabaseTerminalException:
            pass
        qual_name = c.make_qualified_table_name(table_name)
        c.execute_sql(sql.format(table_name=qual_name))


def change_source_table(
    src_pl: dlt.Pipeline, sql: str, table_name: str = "my_source_table"
) -> None:
    with src_pl.sql_client() as c:
        qual_name = c.make_qualified_table_name(table_name)
        c.execute_sql(sql.format(table_name=qual_name))


def show_destination_table(
    dest_pl: dlt.Pipeline,
    table_name: str = "my_source_table",
    column_names: str = "id, val",
) -> None:
    with dest_pl.sql_client() as c:
        dest_qual_name = c.make_qualified_table_name(table_name)
        dest_records = c.execute_sql(f"SELECT {column_names} FROM {dest_qual_name};")
        print(table_name, ":", dest_records)


if __name__ == "__main__":
    replicate_single_table_with_initial_load(
        schema_name="public", table_names="test_table"
    )
    # replicate_single_table_demo()
    # replicate_with_initial_load_demo()
    # replicate_entire_schema_demo()
    # replicate_with_column_selection_demo()
