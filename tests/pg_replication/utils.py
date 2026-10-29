from typing import Sequence, List, Dict, Any, Optional, Tuple

import dlt
from dlt import Pipeline
from dlt.common.data_writers.escape import escape_postgres_identifier
from dlt.common.configuration.specs import ConnectionStringCredentials

from tests.utils import ALL_DESTINATIONS, select_data


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
    qry = f"SELECT {', '.join(column_names)} FROM {table_name}"
    if where_clause is not None:
        qry += " WHERE " + where_clause
    observation = [
        {column_name: row[idx] for idx, column_name in enumerate(column_names)}
        for row in select_data(pipeline, qry)
    ]
    assert sorted(observation, key=lambda d: d[sort_column_name]) == expectation


def run_sql(src_pl: Pipeline, *statements: str) -> None:
    """Runs each statement in its own transaction, `{schema}` is replaced with the dataset."""
    with src_pl.sql_client() as c:
        schema = c.fully_qualified_dataset_name()
        for statement in statements:
            c.execute_sql(statement.replace("{schema}", schema))


def create_tables(src_pl: Pipeline, *statements: str) -> None:
    with src_pl.sql_client() as c:
        c.create_dataset()
    run_sql(src_pl, *statements)


def table_rows(
    pipeline: Pipeline,
    table_name: str,
    column_names: Sequence[str],
    order_by: str = "id",
) -> List[Tuple[Any, ...]]:
    return [
        tuple(row)
        for row in select_data(
            pipeline,
            f"SELECT {', '.join(column_names)} FROM {table_name} ORDER BY {order_by}",
        )
    ]


def is_super_user(sql_client) -> bool:
    """Returns True if Postgres user is superuser, False otherwise."""
    username = dlt.secrets.get(
        "sources.pg_replication.credentials", ConnectionStringCredentials
    ).username
    with sql_client() as c:
        return c.execute_sql(
            f"SELECT rolsuper FROM pg_roles WHERE rolname = '{username}';"
        )[0][0]


def destination_cases(
    cases: Sequence[Tuple[Any, ...]], duckdb_cases: Sequence[Tuple[Any, ...]] = ()
) -> List[Tuple[Any, ...]]:
    """Parametrizes `destination_name` with `cases` on every destination, `duckdb_cases` only on
    duckdb"""
    params = [(name, *case) for name in ALL_DESTINATIONS for case in cases]
    if "duckdb" in ALL_DESTINATIONS:
        params += [("duckdb", *case) for case in duckdb_cases]
    return params
