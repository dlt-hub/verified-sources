from typing import Iterator, Any

import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.common import pendulum

from sql_database import sql_database, sql_table


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
    """Load a few known tables with the standalone sql_table resource"""
    pipeline = dlt.pipeline(
        pipeline_name="rfam_database", destination="postgres", dataset_name="rfam_data"
    )

    # Load a table incrementally starting at a given date
    # Adding incremental via argument like this makes extraction more efficient
    # as only rows newer than the start date are fetched from the table
    family = sql_table(
        table="family",
        incremental=dlt.sources.incremental(
            "updated", initial_value=pendulum.DateTime(2022, 1, 1, 0, 0, 0)
        ),
    )

    # Load all data from another table
    genome = sql_table(table="genome")

    # Run the resources together
    info = pipeline.extract([family, genome], write_disposition="merge")
    print(info)


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
        import connectorx as cx  # type: ignore[import-untyped]

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


if __name__ == "__main__":
    # Load selected tables with different settings
    load_select_tables_from_database()

    # Load tables with the standalone table resource
    # load_standalone_table_resource()

    # Load all tables from the database.
    # Warning: The sample database is very large
    # load_entire_database()
