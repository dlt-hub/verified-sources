"""Replicates postgres tables in batch using logical decoding."""

from typing import Dict, Sequence, Optional

import dlt

from dlt.common.schema.typing import (
    TTableSchemaColumns,
    TAnySchemaColumns,
    TWriteDisposition,
)
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.extract.resource import DltResource
from dlt.extract.typing import TTableHintTemplate

from .helpers import _gen_replication_resource_name, get_pub_ops, replication_items


@dlt.sources.config.with_config(sections=("sources", "pg_replication"))
def replication_resource(
    slot_name: str,
    pub_name: str,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    columns: Optional[Dict[str, TTableHintTemplate[TAnySchemaColumns]]] = None,
    target_batch_size: int = 1000,
    flush_slot: bool = True,
) -> DltResource:
    write_disposition: TWriteDisposition = "append"
    rep_cols: TTableSchemaColumns = {"lsn": {"data_type": "bigint"}}
    resource_name = _gen_replication_resource_name(slot_name, pub_name)

    pub_ops = get_pub_ops(pub_name, credentials)
    if pub_ops["update"] or pub_ops["delete"]:
        write_disposition = "merge"
        rep_cols["lsn"]["dedup_sort"] = "desc"
    if pub_ops["delete"]:
        rep_cols["deleted_ts"] = {"hard_delete": True, "data_type": "timestamp"}

    return dlt.resource(
        replication_items,
        name=resource_name,
        write_disposition=write_disposition,
        columns=rep_cols,
    )(
        credentials=credentials,
        slot_name=slot_name,
        pub_name=pub_name,
        include_columns=include_columns,
        columns=columns,
        target_batch_size=target_batch_size,
        flush_slot=flush_slot,
        write_disposition=write_disposition,
    )
