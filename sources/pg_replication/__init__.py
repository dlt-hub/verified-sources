"""Replicates postgres tables in batch using logical decoding."""

from typing import Dict, Sequence, Optional

import dlt

from dlt.common.schema.typing import TTableSchemaColumns, TWriteDisposition
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.extract.resource import DltResource

from .helpers import _gen_replication_resource_name, get_pub_ops, replication_items


@dlt.sources.config.with_config(sections=("sources", "pg_replication"))
def replication_resource(
    slot_name: str,
    pub_name: str,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    target_batch_size: int = 1000,
    flush_slot: bool = True,
) -> DltResource:
    """Returns a dlt resource that yields data items for changes in one or more postgres tables.

    Relies on a replication slot and publication that publishes DML operations
    (i.e. `insert`, `update`, and/or `delete`). Helper `init_replication` can be
    used to set this up.

    Uses `append` write disposition when the publication only publishes `insert`
    operations, `merge` otherwise.

    Args:
        slot_name (str): Name of the replication slot to consume replication messages from.
        pub_name (str): Name of the publication that publishes DML operations for the table(s).
        credentials (ConnectionStringCredentials): Postgres database credentials.
        include_columns (Optional[Dict[str, Sequence[str]]]): Maps table name(s) to
          sequence of names of columns to include in the generated data items.
          Any column not in the sequence is excluded. If not provided, all columns
          are included. For example:
          ```
          include_columns={
              "table_x": ["col_a", "col_c"],
              "table_y": ["col_x", "col_y", "col_z"],
          }
          ```
        columns (Optional[Dict[str, TTableSchemaColumns]]): Maps
          table name(s) to column hints to apply on the replicated table(s). For example:
          ```
          columns={
              "table_x": {"col_a": {"data_type": "complex"}},
              "table_y": {"col_y": {"precision": 32}},
          }
          ```
        target_batch_size (int): Desired number of data items yielded in a batch.
          Can be used to limit the data items in memory. Note that the number of
          data items yielded can be (far) greater than `target_batch_size`, because
          all messages belonging to the same transaction are always processed in
          the same batch, regardless of the number of messages in the transaction
          and regardless of the value of `target_batch_size`. The number of data
          items can also be smaller than `target_batch_size` when the replication
          slot is exhausted before a batch is full.
        flush_slot (bool): Whether processed messages are discarded from the replication
          slot. Recommended value is True. Be careful when setting False—not flushing
          can eventually lead to a “disk full” condition on the server, because
          the server retains all the WAL segments that might be needed to stream
          the changes via all of the currently open replication slots.

        Returns:
            DltResource that yields data items for changes published in the publication.
    """
    write_disposition: TWriteDisposition = "append"
    resource_name = _gen_replication_resource_name(slot_name, pub_name)

    pub_ops = get_pub_ops(pub_name, credentials)
    if pub_ops["update"] or pub_ops["delete"]:
        write_disposition = "merge"

    return dlt.resource(
        replication_items,
        name=resource_name,
        write_disposition=write_disposition,
    )(
        credentials=credentials,
        slot_name=slot_name,
        pub_name=pub_name,
        include_columns=include_columns,
        columns=columns,
        target_batch_size=target_batch_size,
        flush_slot=flush_slot,
    )
