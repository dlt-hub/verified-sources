"""Replicates postgres tables in batch using logical decoding."""

from typing import Dict, Sequence, Optional, Iterable, Union

import dlt

from dlt.common import logger
from dlt.common.typing import TDataItem
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract.items import DataItemWithMeta
from dlt.sources.credentials import ConnectionStringCredentials

from .helpers import advance_slot, get_max_lsn, ItemGenerator


@dlt.resource(
    name=lambda args: args["slot_name"],
    standalone=True,
)
def replication_resource(
    slot_name: str,
    pub_name: str,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    target_batch_size: int = 1000,
    flush_slot: bool = True,
) -> Iterable[Union[TDataItem, DataItemWithMeta]]:
    """Resource yielding data items for changes in one or more postgres tables.

    - Relies on a replication slot and publication that publishes DML operations
    (i.e. `insert`, `update`, and/or `delete`). Helper `init_replication` can be
    used to set this up.
    - Maintains LSN of last consumed message in state to track progress.
    - At start of the run, advances the slot upto last consumed message in previous run.
    - Processes in batches to limit memory usage.

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
        columns (Optional[Dict[str, TTableHintTemplate[TAnySchemaColumns]]]): Maps
          table name(s) to column hints to apply on the replicated table(s). For example:
          ```
          columns={
              "table_x": {"col_a": {"data_type": "json"}},
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

        Yields:
            Data items for changes published in the publication.
    """
    # start where we left off in previous run
    start_lsn = dlt.current.resource_state().get("last_commit_lsn", 0)
    if flush_slot and start_lsn:
        advance_slot(start_lsn, slot_name, credentials)

    # continue until last message in replication slot
    options = {"publication_names": pub_name, "proto_version": "1"}
    upto_lsn = get_max_lsn(slot_name, options, credentials)
    if upto_lsn is None:
        return
    logger.info(
        f"Replicating slot {slot_name} publication {pub_name} from {start_lsn} to {upto_lsn}"
    )
    # generate items in batches
    while True:
        gen = ItemGenerator(
            credentials=credentials,
            slot_name=slot_name,
            options=options,
            upto_lsn=upto_lsn,
            start_lsn=start_lsn,
            target_batch_size=target_batch_size,
            include_columns=include_columns,
            columns=columns,
        )
        yield from gen
        if gen.generated_all:
            dlt.current.resource_state()["last_commit_lsn"] = gen.last_commit_lsn
            break
        start_lsn = gen.last_commit_lsn
