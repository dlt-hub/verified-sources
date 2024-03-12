from typing import Optional, Sequence, Dict, Iterable

import dlt

from dlt.common.schema.typing import TAnySchemaColumns
from dlt.sources import DltResource
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.extract.typing import TTableHintTemplate

from .helpers import (
    init_table_replication,
    table_changes,
    snapshot_and_changes,
    PgReplicationCredentialsConfiguration,
    InitTableReplicationConfig,
    ReplicatedTableConfig,
)


@dlt.sources.config.with_config(
    sections=("sources", "pg_replication"), spec=PgReplicationCredentialsConfiguration
)
def replicated_table(
    table_name: str,
    schema_name: str,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    columns: TTableHintTemplate[TAnySchemaColumns] = None,
    include_columns: Optional[Sequence[str]] = None,
    init_conf: Optional[InitTableReplicationConfig] = None,
    slot_name: Optional[str] = None,
    pub_name: Optional[str] = None,
    table_snapshot: Optional[DltResource] = None,
    target_batch_size: int = 1000,
    flush_slot: bool = True,
) -> DltResource:
    """Returns a dlt resource that yields data for a replicated postgres table.

    This resource handles both the one-off initial load and subsequent changes
    to the table. The initial load is optional and enabled by setting "persist_snapshot"
    to True in `init_conf`, or by providing `table_snapshot` directly.

    Replication initialization is handled automatically if `slot_name` is not provided.
    In that case, this function calls `init_table_replication`, which creates a
    new dedicated replication slot and publication for the table, or returns the names of
    the replication slot and publication if they already exist.

    Args:
        table_name (str): Name of the table to replicate.
        schema_name (str): Name of the schema the table belongs to.
        credentials (ConnectionStringCredentials): Postgres database credentials.
        columns (TTableHintTemplate[TAnySchemaColumns]): Column hints for the DltResource.
        include_columns (Optional[Sequence[str]]): Sequence of names of
          columns to include in the replicated table. Any columns not in the sequence
          are excluded. If not provided, all columns are included in the table.
        init_conf (Optional[InitTableReplicationConfig]): Dictionary to configure
          the initialization of the table replication. Key-value pairs in the dictionary
          are passed as keyword arguments to `init_table_replication`. Allowed keys
          are "publish", "persist_snapshot", and "reset".
          Example: {"publish": "insert, update", "persist_snapshot": True}.
        slot_name (Optional[str]): Name of the replication slot to consume replication
          messages from. Each table is expected to have a dedicated slot. If not
          provided, `init_table_replication` is called, which creates a new replication slot
          and publication for the table, or returns the names of the replication slot
          and publication if they already exist.
        pub_name (Optional[str]): Name of the publication that published DML operations
          for the table. Each table is expected to have a dedicated publication.
        table_snapshot (Optional[DltResource]): Resource yielding data items from
          a snapshot exported during creation of a replication slot. This resource
          can be created using `init_table_replication`. Only relevant if the
          initialization of the table replication is not handled by this function.
        target_batch_size (int): Desired number of data items (including metadata
          items) yielded in a batch. This argument can be used to limit the data
          items in memory. Note that the number of data items yielded can be (far)
          greater than `min_batch_size`, because all messages belonging to the same
          transaction are always processed in the same batch, regardless of the number
          of messages in the transaction and regardless of the value of `min_batch_size`.
          The number of data items can also be smaller than `min_batch_size` when
          the replication slot is exhausted before a batch is full. This argument
          is passed to `table_changes` and does not apply to the `table_snapshot` resource.
        flush_slot (bool): Whether processed messages are discarded from the replication
          slot. The recommended value is True. Be careful when setting this argument
          to False—not flushing can eventually lead to a “disk full” condition on the server,
          because the server retains all the WAL segments that might be needed to
          stream the changes via all of the currently open replication slots.

    Returns:
        DltResource that yields data items for the initial load and subsequent
        changes in the postgres table.
    """
    if slot_name is None:
        slot_name, pub_name, table_snapshot = init_table_replication(
            table_name=table_name,
            schema_name=schema_name,
            columns=columns,
            include_columns=include_columns,
            **(dict() if init_conf is None else init_conf)
        )

    changes = table_changes(
        credentials=credentials,
        table_name=table_name,
        schema_name=schema_name,
        include_columns=include_columns,
        target_batch_size=target_batch_size,
        slot_name=slot_name,
        pub_name=pub_name,
        flush_slot=flush_slot,
    )

    # include `dedup_sort` and `hard_delete` hints defined on table_changes resource
    if columns is None:
        columns = changes.columns
    else:
        columns = {**columns, **changes.columns}  # type: ignore[dict-item]

    # return combined resource that first yields from table_snapshot, then from table_changes
    resource_name = schema_name + "_" + table_name
    return dlt.resource(
        # combined,
        snapshot_and_changes,
        name=resource_name,
        table_name=table_name,
        write_disposition=changes.write_disposition,
        columns=columns,
        primary_key=changes._hints.get("primary_key"),
    )(resource_name, table_snapshot, changes)


@dlt.source(name="pg_replication")
def pg_replication_source(
    table_names: Sequence[str] = dlt.config.value,
    schema_name: str = dlt.config.value,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    conf: Optional[Dict[str, ReplicatedTableConfig]] = None,
) -> Iterable[DltResource]:
    """A dlt source that yields resources for one or more replicated postgres tables.

    Args:
        table_names (Sequence[str]): Sequences of names of tables to replicate.
        schema_name (str): Name of the schema the tables belong to.
        credentials (ConnectionStringCredentials): Postgres database credentials.
        conf (Dict[str, ReplicatedTableConfig]): Mapping from table names to
          ReplicatedTableConfig objects, which are configuration dictionaries for
          the resources created for individual tables. Key-value pairs in the dictionary
          are passed as keyword arguments to `replicated_table`. Allowed keys
          are "columns", "include_columns", "target_batch_size", "init_conf", "slot_name",
          "pub_name", "flush_slot", and "table_snapshot". See the documentation for
          the `replicated_table` function for an explanation of these arguments.
          Example: {"table_x": {"include_columns": ["id", "val"]}, "table_y":
          {"init_conf": {"publish": "insert", "persist_snapshot": True}}}.

    Yields:
        A DltResource for each replicated postgres table.
    """
    for table_name in table_names:
        yield replicated_table(
            table_name=table_name,
            schema_name=schema_name,
            credentials=credentials,
            **(dict() if conf is None or table_name not in conf else conf[table_name])
        )
