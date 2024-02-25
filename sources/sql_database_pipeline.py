from typing import Iterator, Any

import dlt
from dlt.sources.credentials import ConnectionStringCredentials

from sql_database import sql_database, sql_table, Table


def load_select_tables_from_database() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it.

    This example sources data from the public Rfam MySQL database.
    """
    # Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rfam", destination="postgres", dataset_name="rfam_data"
    )

    # Credentials for the sample database.
    # Note: It is recommended to configure credentials in `.dlt/secrets.toml` under `sources.sql_database.credentials`
    credentials = ConnectionStringCredentials(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    )

    # Configure the source to load a few select tables incrementally
    source_1 = sql_database(credentials).with_resources("family", "clan")
    # Add incremental config to the resources. "updated" is a timestamp column in these tables that gets used as a cursor
    source_1.family.apply_hints(incremental=dlt.sources.incremental("updated"))
    source_1.clan.apply_hints(incremental=dlt.sources.incremental("updated"))

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(source_1, write_disposition="merge")
    print(info)

    # Load some other tables with replace write disposition. This overwrites the existing tables in destination
    source_2 = sql_database(credentials).with_resources("features", "author")
    info = pipeline.run(source_2, write_disposition="replace")
    print(info)

    # Load a table incrementally with append write disposition
    # this is good when a table only has new rows inserted, but not updated
    source_3 = sql_database(credentials).with_resources("genome")
    source_3.genome.apply_hints(incremental=dlt.sources.incremental("created"))

    info = pipeline.run(source_3, write_disposition="append")
    print(info)


def load_entire_database() -> None:
    """Use the sql_database source to completely load all tables in a database"""
    pipeline = dlt.pipeline(
        pipeline_name="rfam", destination="postgres", dataset_name="rfam_data"
    )

    # By default the sql_database source reflects all tables in the schema
    # The database credentials are sourced from the `.dlt/secrets.toml` configuration
    source = sql_database()

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")
    print(info)


def load_standalone_table_resource() -> None:
    """Load a few known tables with the standalone sql_table resource, request full schema and deferred
    table reflection"""
    pipeline = dlt.pipeline(
        pipeline_name="rfam_database",
        destination="duckdb",
        dataset_name="rfam_data",
        full_refresh=True,
    )

    # Load a table incrementally starting at a given date
    # Adding incremental via argument like this makes extraction more efficient
    # as only rows newer than the start date are fetched from the table
    # we also use `detect_precision_hints` to get detailed column schema
    # and defer_table_reflect to reflect schema only during execution
    family = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="family",
        incremental=dlt.sources.incremental(
            "updated",
        ),
        detect_precision_hints=True,
        defer_table_reflect=True,
    )
    # columns will be empty here due to defer_table_reflect set to True
    print(family.compute_table_schema())

    # Load all data from another table
    genome = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="genome",
        detect_precision_hints=True,
        defer_table_reflect=True,
    )

    # Run the resources together
    info = pipeline.extract([family, genome], write_disposition="merge")
    print(info)
    # Show inferred columns
    print(pipeline.default_schema.to_pretty_yaml())


def select_columns() -> None:
    """Uses table adapter callback to modify list of columns to be selected"""
    pipeline = dlt.pipeline(
        pipeline_name="rfam_database",
        destination="duckdb",
        dataset_name="rfam_data_cols",
        full_refresh=True,
    )

    def table_adapter(table: Table) -> None:
        print(table.name)
        if table.name == "family":
            # this is SqlAlchemy table. _columns are writable
            # let's drop updated column
            table._columns.remove(table.columns["updated"])

    family = sql_table(
        credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table="family",
        chunk_size=10,
        detect_precision_hints=True,
        table_adapter_callback=table_adapter,
    )

    # also we do not want the whole table, so we add limit to get just one chunk (10 records)
    pipeline.run(family.add_limit(1))
    # only 10 rows
    print(pipeline.last_trace.last_normalize_info)
    # no "updated" column in "family" table
    print(pipeline.default_schema.to_pretty_yaml())


def reflect_and_connector_x() -> None:
    """Uses sql_database to reflect the table schema and then connectorx to load it. Connectorx has rudimentary type support ie.
    is not able to use decimal types and is not providing length information for text and binary types.

    NOTE: mind that for DECIMAL/NUMERIC the data is converted into float64 and then back into decimal Python type. Do not use it
    when decimal representation is important ie. when you process currency.
    """

    # uncomment line below to get load_id into your data (slows pyarrow loading down)
    dlt.config["normalize.parquet_normalizer.add_dlt_load_id"] = True

    # Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rfam_cx", destination="postgres", dataset_name="rfam_data_cx"
    )

    # Credentials for the sample database.
    credentials = ConnectionStringCredentials(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    )

    # below we reflect family and genome tables. detect_precision_hints is set to True to emit
    # full metadata
    sql_alchemy_source = sql_database(
        credentials, detect_precision_hints=True
    ).with_resources("family", "genome")

    # display metadata
    print(sql_alchemy_source.family.columns)
    print(sql_alchemy_source.genome.columns)

    # define a resource that will be used to read data from connectorx
    @dlt.resource
    def read_sql_x(
        conn_str: str,
        query: str,
    ) -> Iterator[Any]:
        import connectorx as cx  # type: ignore

        yield cx.read_sql(
            conn_str,
            query,
            return_type="arrow2",
            protocol="binary",
        )

    # Option 1: use columns from sql_alchemy source to define columns for connectorx
    genome_cx = read_sql_x(
        "mysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        "SELECT * FROM genome LIMIT 100",
    ).with_name("genome")
    family_cx = read_sql_x(
        "mysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        "SELECT * FROM family LIMIT 100",
    ).with_name("family")

    # Option 1: use columns from sql_alchemy source to define columns for connectorx
    genome_cx.apply_hints(columns=sql_alchemy_source.genome.columns)
    family_cx.apply_hints(columns=sql_alchemy_source.family.columns)

    info = pipeline.run([genome_cx, family_cx])
    print(info)
    print(pipeline.default_schema.to_pretty_yaml())

    # Option 2: replace sql alchemy date generator with connectorx data generator (this is hacky)
    sql_alchemy_source.genome._pipe.replace_gen(genome_cx._pipe.gen)
    sql_alchemy_source.family._pipe.replace_gen(family_cx._pipe.gen)
    info = pipeline.run(sql_alchemy_source)
    print(info)


def tmp() -> None:
    from sql_database.pg_cdc_utils import (
        cdc_rows,
        rep_conn,
        create_publication,
        add_table_to_publication,
        create_replication_slot,
        get_max_lsn,
    )

    DATABASE = "dlt_data"
    USER = "replication_reader" # CREATE USER replication_reader WITH PASSWORD 'replication_reader' LOGIN REPLICATION;
    # USER = "loader"
    PASSWORD = "replication_reader"
    # PASSWORD = "loader"
    HOST = "LOCALHOST"
    PORT = '5432'
    

    conn = rep_conn(
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
    )

    publication_name = "foo"
    table_name = "tmp"
    slot_name = "bar"  # "foo"
    options = {'publication_names': publication_name, 'proto_version': '1'}

    cur = conn.cursor()
    # cur.drop_replication_slot(slot_name)
    create_replication_slot(slot_name, cur)
    create_publication(publication_name, cur)
    add_table_to_publication(table_name, publication_name, cur)
    max_lsn = get_max_lsn(slot_name, options, cur)
    print(max_lsn)
    cur.start_replication(slot_name=slot_name, decode=False, options=options)
    
    assert False
    slot_name = "foo"

    for row in cdc_rows(conn):
        print(row)

    # pipeline = dlt.pipeline(
    #     pipeline_name="tmp",
    #     destination='duckdb',
    #     dataset_name="tmp",
    #     pipelines_dir="tmp",
    #     full_refresh=True,
    # )

    # tbl = sql_table(
    #     credentials="postgresql://loader:loader@localhost:5432/dlt_data",
    #     schema="tmp",
    #     table="tmp",
    # )

    # pipeline.run(tbl)
    # print(pipeline.last_trace.last_normalize_info)
    # print(pipeline.default_schema.to_pretty_yaml())    


if __name__ == "__main__":
    # Load selected tables with different settings
    # load_select_tables_from_database()

    # load a table and select columns
    # select_columns()
    tmp()

    # Load tables with the standalone table resource
    # load_standalone_table_resource()

    # Load all tables from the database.
    # Warning: The sample database is very large
    # load_entire_database()
