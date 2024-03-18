import dlt


from pg_replication import replication_resource
from pg_replication.helpers import init_replication


def replicate_single_table() -> None:
    """Sets up replication for a single Postgres table and loads changes into a destination.

    Demonstrates basic usage of `init_replication` helper and `replication_resource` resource.
    Uses `src_pl` to create and change the replicated Postgres table—this
    is only for demonstration purposes, you won't need this when you run in production
    as you'll probably have another process feeding your Postgres instance.
    """
    # create source and destination pipelines
    src_pl = dlt.pipeline(
        pipeline_name="replicate_single_table_src_pl",
        destination="postgres",
        dataset_name="replicate_single_table",
        full_refresh=True,
    )
    dest_pl = dlt.pipeline(
        pipeline_name="replicate_single_table_dest_pl",
        destination="duckdb",
        dataset_name="replicate_single_table",
        full_refresh=True,
    )

    # create table in source to demonstrate replication
    create_source_table(
        src_pl, "CREATE TABLE {table_name} (id integer PRIMARY KEY, val bool);"
    )

    # initialize replication for the source table—this creates a replication slot and publication
    slot_name = "replicate_single_table_slot"
    pub_name = "replicate_single_table_pub"
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


# define some helper methods to make examples more readable


def create_source_table(src_pl: dlt.Pipeline, sql: str) -> None:
    with src_pl.sql_client() as c:
        c.create_dataset()
        qual_name = c.make_qualified_table_name("my_source_table")
        c.execute_sql(sql.format(table_name=qual_name))


def change_source_table(src_pl: dlt.Pipeline, sql: str) -> None:
    with src_pl.sql_client() as c:
        qual_name = c.make_qualified_table_name("my_source_table")
        c.execute_sql(sql.format(table_name=qual_name))


def show_destination_table(dest_pl: dlt.Pipeline) -> None:
    with dest_pl.sql_client() as c:
        dest_qual_name = c.make_qualified_table_name("my_source_table")
        dest_records = c.execute_sql(f"SELECT id, val FROM {dest_qual_name};")
        print(dest_records)


if __name__ == "__main__":
    replicate_single_table()
