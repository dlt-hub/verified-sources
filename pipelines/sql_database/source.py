from typing import List, Iterator, Dict, Any, Optional

import dlt
from dlt.extract.source import DltResource
from dlt.common import json

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import Engine, Connection


def table_rows(engine: Engine, table: Table) -> Iterator[Dict[str, Any]]:
    """Yields rows from the given database table.

    :param engine: An `sqlalchemy.engine.Engine` instance configured for the database
    :param table: The table to load data from
    """
    conn: Connection
    with engine.connect() as conn:
        with conn.execution_options(yield_per=1000).execute(table.select()) as result:
            for partition in result.partitions():  # type: ignore
                for row in partition:
                    yield dict(row._mapping)


@dlt.source
def sql_database(
    database_url: str = dlt.secrets.value,
    schema: Optional[str] = dlt.config.value,
    table_names: Optional[List[str]] = dlt.config.value,
    write_disposition: str = 'replace'
) -> List[DltResource]:
    """A dlt source which loads data from an SQL database using SQLAlchemy.
    Resources are automatically created for each table in the schema or from the given list of tables.

    :param database_url: An SQLAlchemy database connection string (e.g. `postgresql://user:password@localhost/my_database`)
    :param schema: Name of the database schema to load (if different from default)
    :param table_names: A list of table names to load. By default all defaults in the schema are loaded.

    :return: A list of dlt resources for each table to be loaded
    """
    engine = create_engine(database_url)
    engine.execution_options(stream_results=True)
    metadata = MetaData(
        schema=schema,
    )
    metadata.reflect(bind=engine)

    tables = [
        table
        for table in metadata.tables.values()
        if not table_names or table.name in table_names
    ]
    return [
        dlt.resource(table_rows, name=table.name)(
            engine, table
        )
        for table in tables
    ]
