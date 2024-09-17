"""Replicates postgres tables in batch using logical decoding."""

from collections import defaultdict
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence, Union

import dlt
from dlt.extract import DltResource
from dlt.extract.items import TDataItem
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_table

from .helpers import (
    BackendHandler,
    ItemGenerator,
    ReplicationOptions,
    SqlTableOptions,
    advance_slot,
    cleanup_snapshot_resources,
    configure_engine,
    create_replication_slot,
    drop_replication_slot,
    get_max_lsn,
    get_rep_conn,
)


@dlt.source
def replication_source(
    slot_name: str,
    schema: str,
    table_names: Union[str, Sequence[str]],
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    repl_options: Optional[Mapping[str, ReplicationOptions]] = None,
    target_batch_size: int = 1000,
    flush_slot: bool = True,
) -> Iterable[DltResource]:
    """
    Defines a dlt source for replicating Postgres tables using logical replication.
    This source reads from a replication slot and pipes the changes using transformers.

    - Relies on a replication slot that publishes DML operations (i.e. `insert`, `update`, and `delete`).
    - Maintains LSN of last consumed message in state to track progress.
    - At start of the run, advances the slot upto last consumed message in previous run (for pg>10 only)
    - Processes in batches to limit memory usage.

    Args:
        slot_name (str):
            The name of the logical replication slot used to fetch WAL changes.
        schema (str):
            Name of the schema to replicate tables from.
        table_names (Union[str, Sequence[str]]):
            The name(s) of the tables to replicate. Can be a single table name or a list of table names.
        credentials (ConnectionStringCredentials):
            Database credentials for connecting to the Postgres instance.
        repl_options (Optional[Mapping[str, ReplicationOptions]], optional):
            A mapping of table names to `ReplicationOptions`, allowing for fine-grained control over
            replication behavior for each table.

            Each `ReplicationOptions` dictionary can include the following keys:
                - `backend` (Optional[TableBackend]): Specifies the backend to use for table replication.
                - `backend_kwargs` (Optional[Mapping[str, Any]]): Additional configuration options for the backend.
                - `column_hints` (Optional[TTableSchemaColumns]): A dictionary of hints for column types or properties.
                - `include_lsn` (Optional[bool]): Whether to include the LSN (Log Sequence Number)
                  in the replicated data. Defaults to `True`.
                - `include_deleted_ts` (Optional[bool]): Whether to include a timestamp for deleted rows.
                  Defaults to `True`.
                - `include_commit_ts` (Optional[bool]): Whether to include the commit timestamp of each change.
                - `include_tx_id` (Optional[bool]): Whether to include the transaction ID of each change.
                - `included_columns` (Optional[Set[str]]): A set of specific columns to include in the replication.
                  If not specified, all columns are included.
        target_batch_size (int, optional):
            The target size of each batch of replicated data items. Defaults to `1000`.
        flush_slot (bool, optional):
            If `True`, advances the replication slot to the last processed LSN
            to prevent replaying already replicated changes. Defaults to `True`.

    Yields:
        Iterable[DltResource]:
            A collection of `DltResource` objects, each corresponding to a table being replicated.

    Notes:
        - The `repl_options` parameter allows fine-tuning of replication behavior, such as column filtering
          or write disposition configuration, per table.
        - The replication process is incremental, ensuring only new changes are processed after the last commit LSN.
    """
    table_names = [table_names] if isinstance(table_names, str) else table_names or []
    repl_options = defaultdict(lambda: ReplicationOptions(), repl_options or {})

    @dlt.resource(name=lambda args: args["slot_name"], standalone=True)
    def replication_resource(slot_name: str) -> Iterable[TDataItem]:
        # start where we left off in previous run
        start_lsn = dlt.current.resource_state().get("last_commit_lsn", 0)
        if flush_slot and start_lsn > 0:
            advance_slot(start_lsn, slot_name, credentials)

        # continue until last message in replication slot
        upto_lsn = get_max_lsn(credentials)
        if upto_lsn is None:
            return

        table_qnames = {f"{schema}.{table_name}" for table_name in table_names}

        # generate items in batches
        while True:
            gen = ItemGenerator(
                credentials=credentials,
                slot_name=slot_name,
                table_qnames=table_qnames,
                upto_lsn=upto_lsn,
                start_lsn=start_lsn,
                repl_options=repl_options,
                target_batch_size=target_batch_size,
            )
            yield from gen
            if gen.generated_all:
                dlt.current.resource_state()["last_commit_lsn"] = gen.last_commit_lsn
                break
            start_lsn = gen.last_commit_lsn

    wal_reader = replication_resource(slot_name)

    for table in table_names:
        yield dlt.transformer(
            _create_table_dispatch(table, repl_options=repl_options[table]),
            data_from=wal_reader,
            name=table,
        )


def _create_table_dispatch(
    table: str, repl_options: ReplicationOptions
) -> Callable[[TDataItem], Any]:
    """Creates a dispatch handler that processes data items based on a specified table and optional column hints."""
    handler = BackendHandler(table, repl_options)
    # FIXME Uhhh.. why do I have to do this?
    handler.__qualname__ = "BackendHandler.__call__"  # type: ignore[attr-defined]
    return handler


@dlt.source
def init_replication(
    slot_name: str,
    schema: str,
    table_names: Optional[Union[str, Sequence[str]]] = None,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    take_snapshots: bool = False,
    table_options: Optional[Mapping[str, SqlTableOptions]] = None,
    reset: bool = False,
) -> Iterable[DltResource]:
    """
    Initializes a replication session for Postgres using logical replication.
    Optionally, snapshots of specified tables can be taken during initialization.

    Args:
        slot_name (str):
            The name of the logical replication slot to be used or created.
        schema (str):
            Name of the schema to replicate tables from.
        table_names (Optional[Union[str, Sequence[str]]]):
            The name(s) of the table(s) to replicate. Can be a single table name or a list of table names.
            If not provided, no tables will be replicated unless `take_snapshots` is `True`.
        credentials (ConnectionStringCredentials):
            Database credentials for connecting to the Postgres instance.
        take_snapshots (bool):
            Whether to take initial snapshots of the specified tables.
            Defaults to `False`.
        table_options (Optional[Mapping[str, SqlTableOptions]]):
            Additional options for configuring replication for specific tables.
            These are the exact same parameters for the `dlt.sources.sql_database.sql_table` function.
            Argument is only used if `take_snapshots` is `True`.
        reset (bool, optional):
            If `True`, drops the existing replication slot before creating a new one.
            Use with caution, as this will clear existing replication state.
            Defaults to `False`.

    Returns:
        - None if `take_snapshots` is `False`
        - a list of `DltResource` objects for the snapshot table(s) if `take_snapshots` is `True`.

    Notes:
        - If `reset` is `True`, the existing replication slot will be dropped before creating a new one.
        - When `take_snapshots` is `True`, the function configures a snapshot isolation level for consistent table snapshots.
    """
    rep_conn = get_rep_conn(credentials)
    rep_cur = rep_conn.cursor()
    if reset:
        drop_replication_slot(slot_name, rep_cur)
    slot = create_replication_slot(slot_name, rep_cur)

    # Close connection if no snapshots are needed
    if not take_snapshots:
        rep_conn.close()
        return

    assert table_names is not None

    engine = configure_engine(
        credentials, rep_conn, slot.get("snapshot_name") if slot else None
    )

    table_names = [table_names] if isinstance(table_names, str) else table_names or []

    for table in table_names:
        table_args = (table_options or {}).get(table, {}).copy()
        yield sql_table(credentials=engine, table=table, schema=schema, **table_args)


__all__ = [
    "ReplicationOptions",
    "cleanup_snapshot_resources",
    "init_replication",
    "replication_source",
]
