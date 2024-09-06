import dlt

from dlt.common.destination import Destination
from dlt.destinations.impl.postgres.configuration import PostgresCredentials

from pg_replication import replication_resource
from pg_replication.helpers import init_replication


PG_CREDS = dlt.secrets.get("sources.pg_replication.credentials", PostgresCredentials)


def replicate_single_table() -> None:
    """Sets up replication for a single Postgres table and loads changes into a destination.

    Demonstrates basic usage of `init_replication` helper and `replication_resource` resource.
    Uses `src_pl` to create and change the replicated Postgres table—this
    is only for demonstration purposes, you won't need this when you run in production
    as you'll probably have another process feeding your Postgres instance.
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


def replicate_with_initial_load() -> None:
    """Sets up replication with initial load.

    Demonstrates usage of `persist_snapshots` argument and snapshot resource
    returned by `init_replication` helper.
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


def replicate_entire_schema() -> None:
    """Demonstrates setup and usage of schema replication.

    Schema replication requires a Postgres server version of 15 or higher. An
    exception is raised if that's not the case.
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


def replicate_with_column_selection() -> None:
    """Sets up replication with column selection.

    Demonstrates usage of `include_columns` argument.
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
    replicate_single_table()
    # replicate_with_initial_load()
    # replicate_entire_schema()
    # replicate_with_column_selection()
