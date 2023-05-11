from typing import List, Optional, Union, Any
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine

import dlt
from dlt.extract.source import DltResource
from dlt.common.configuration.specs import BaseConfiguration, configspec
from dlt.common.schema.typing import TWriteDisposition

from dlt.sources.credentials import ConnectionStringCredentials

from .util import table_rows, engine_from_credentials, get_primary_key


@configspec
class SqlDatabaseTableConfiguration(BaseConfiguration):
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]


@configspec
class SqlTableResourceConfiguration(BaseConfiguration):
    credentials: ConnectionStringCredentials
    table: str
    incremental: Optional[dlt.sources.incremental] = None  # type: ignore[type-arg]
    schema: Optional[str]


@dlt.common.configuration.with_config(sections=('sources', 'sql_database'), spec=SqlTableResourceConfiguration)
def sql_table(
    credentials: Union[ConnectionStringCredentials, Engine, str] = dlt.secrets.value,
    table: str = dlt.config.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[MetaData] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
) -> DltResource:
    """A dlt resource which loads data from an SQL database table using SQLAlchemy.

    :param credentials: Database credentials or an `sqlalchemy.Engine` instance
    :param table: Name of the table to load
    :param schema: Optional name of the schema table belongs to (uses databse default schema by default)
    :param metadata: Optional `sqlalchemy.MetaData` instance. `schema` argument is ignored when this is used.
    :param incremental: Option to enable incremental loading for the table. E.g. `incremental=dlt.source.incremental('updated_at', pendulum.parse('2022-01-01T00:00:00Z'))`
    :param write_disposition: Write disposition of the resource
    """
    engine = engine_from_credentials(credentials)
    engine.execution_options(stream_results=True)
    metadata = metadata or MetaData(schema=schema)

    table_obj = Table(table, metadata, autoload_with=engine)

    return dlt.resource(
        table_rows, name=table_obj.name, primary_key=get_primary_key(table_obj)
    )(engine, table_obj, incremental=incremental)


@dlt.source
def sql_database(
    credentials: Union[ConnectionStringCredentials, Engine, str] = dlt.secrets.value,
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
            name=table.name,
            primary_key=get_primary_key(table),
            spec=SqlDatabaseTableConfiguration,
        )(engine, table)
        for table in tables
    ]
