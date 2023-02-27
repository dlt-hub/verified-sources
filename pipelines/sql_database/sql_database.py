from typing import List, Iterator, Dict, Any, Optional

import dlt
from dlt.extract.source import DltResource
from dlt.common.configuration.specs import ConnectionStringCredentials
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import Engine

from pipelines.sql_database.util import table_rows


@dlt.source
def sql_database(
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    schema: Optional[str] = dlt.config.value,
    table_names: Optional[List[str]] = dlt.config.value,
    write_disposition: str = 'replace',
) -> List[DltResource]:
    """A dlt source which loads data from an SQL database using SQLAlchemy.
    Resources are automatically created for each table in the schema or from the given list of tables.

    :param credentials: Credentials for the database to load
    :param schema: Name of the database schema to load (if different from default)
    :param table_names: A list of table names to load. By default all tables in the schema are loaded.

    :return: A list of dlt resources for each table to be loaded
    """
    engine = create_engine(credentials.to_native_representation())
    engine.execution_options(stream_results=True)
    metadata = MetaData(schema=schema)

    if table_names:
        tables = [
            Table(name, metadata, autoload_with=engine)
            for name in table_names
        ]
    else:
        metadata.reflect(bind=engine)
        tables = list(metadata.tables.values())

    return [
        dlt.resource(table_rows, name=table.name)(
            engine, table
        )
        for table in tables
    ]
