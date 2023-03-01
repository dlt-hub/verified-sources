from typing import List, Iterator, Dict, Any, Optional, Union
from functools import partial

import dlt
from dlt.extract.source import DltResource
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.schema.typing import TWriteDisposition
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import Engine

from pipelines.sql_database.util import table_rows, engine_from_credentials


@dlt.resource
def sql_table(
    credentials: Union[ConnectionStringCredentials, Engine] = dlt.secrets.value,
    table: str = dlt.config.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[MetaData] = None,
    cursor_column: Optional[str] = dlt.config.value,
    unique_column: Optional[str] = dlt.config.value,
    write_disposition: TWriteDisposition = 'append'
) -> DltResource:
    """A dlt resource which loads data from an SQL database table using SQLAlchemy.

    :param credentials: Database credentials or an `sqlalchemy.Engine` instance
    :param table: Name of the table to load
    :param schema: Optional name of the schema table belongs to (uses databse default schema by default)
    :param metadata: Optional `sqlalchemy.MetaData` instance. `schema` argument is ignored when this is used.
    :param cursor_column: Optional column name to use as cursor for resumeable loading
    :param unique_column: Optional column that uniquely identifies a row in the table for resumeable loading
    :param write_disposition: Write disposition of the resource
    """
    engine = engine_from_credentials(credentials)
    engine.execution_options(stream_results=True)
    metadata = metadata or MetaData(schema=schema)

    table_obj = Table(table, metadata, autoload_with=engine)

    return dlt.resource(
        table_rows(engine, table_obj, cursor_column=cursor_column, unique_column=unique_column),
        name=table_obj.name, write_disposition=write_disposition
    )


@dlt.source
def sql_database(
    credentials: Union[ConnectionStringCredentials, Engine] = dlt.secrets.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[MetaData] = None,
    table_names: Optional[List[str]] = dlt.config.value,
) -> List[DltResource]:
    """A dlt source which loads data from an SQL database using SQLAlchemy.
    Resources are automatically created for each table in the schema or from the given list of tables.

    :param credentials: Database credentials or an `sqlalchemy.Engine` instance
    :param schema: Name of the database schema to load (if different from default)
    :param metadata: Optional `sqlalchemy.MetaData` instance. `schema` argument is ignored when this is used.
    :param table_names: A list of table names to load. By default all tables in the schema are loaded.

    :return: A list of dlt resources for each table to be loaded
    """
    engine = engine_from_credentials(credentials)
    engine.execution_options(stream_results=True)
    metadata = metadata or MetaData(schema=schema)

    if table_names:
        tables = [
            Table(name, metadata, autoload_with=engine)
            for name in table_names
        ]
    else:
        metadata.reflect(bind=engine)
        tables = list(metadata.tables.values())

    return [
        dlt.resource(
            table_rows,
            name=table.name
        )(engine, table)
        for table in tables
    ]
