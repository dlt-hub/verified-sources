from typing import Sequence, List, Dict, Any, Optional

from dlt import Pipeline
from dlt.common.data_writers.escape import escape_postgres_identifier

from tests.utils import select_data


def add_pk(sql_client, table_name: str, column_name: str) -> None:
    """Adds primary key to postgres table.

    In the context of replication, the primary key serves as REPLICA IDENTITY.
    A REPLICA IDENTITY is required when publishing UPDATEs and/or DELETEs.
    """
    with sql_client() as c:
        qual_name = c.make_qualified_table_name(table_name)
        c.execute_sql(f"ALTER TABLE {qual_name} ADD PRIMARY KEY ({column_name});")


def assert_loaded_data(
    pipeline: Pipeline,
    table_name: str,
    column_names: Sequence[str],
    expectation: List[Dict[str, Any]],
    sort_column_name: str,
    where_clause: Optional[str] = None,
) -> None:
    """Asserts loaded data meets expectation."""
    qual_name = pipeline.sql_client().make_qualified_table_name(table_name)
    column_str = ", ".join(map(escape_postgres_identifier, column_names))
    qry = f"SELECT {column_str} FROM {qual_name}"
    if where_clause is not None:
        qry += " WHERE " + where_clause
    observation = [
        {column_name: row[idx] for idx, column_name in enumerate(column_names)}
        for row in select_data(pipeline, qry)
    ]
    assert sorted(observation, key=lambda d: d[sort_column_name]) == expectation
