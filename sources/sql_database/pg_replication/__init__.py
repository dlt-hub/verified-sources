from typing import Optional, Sequence

import dlt

from dlt.common.schema.typing import TColumnNames
from dlt.sources import DltResource
from dlt.sources.credentials import ConnectionStringCredentials

from .helpers import table_replication_items, TableChangesResourceConfiguration


@dlt.sources.config.with_config(
    sections=("sources", "sql_database"),
    spec=TableChangesResourceConfiguration,
)
def table_changes(
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    table: str = dlt.config.value,
    primary_key: TColumnNames = None,
    include_columns: Optional[Sequence[str]] = dlt.config.value,
    slot_name: str = dlt.config.value,
    publication_name: str = dlt.config.value,
    upto_lsn: Optional[int] = None,
) -> DltResource:
    """Returns a dlt resource that yields data items for changes in a postgres table.

    Relies on a dedicated replication slot and publication that publishes DML
    operations (i.e. `insert`, `update`, and/or `delete`) for the table (helper
    method `init_table_replication` can be used to set this up).
    Uses `merge` write disposition to merge changes into destination table(s).

    Args:
        credentials (ConnectionStringCredentials): Postgres database credentials.
        table (str): Name of the table that is replicated
        primary_key (TColumnNames): Names of one or multiple columns serving as
          primary key on the table. Used to deduplicate data items in the `merge`
          operation.
        include_columns (Optional[Sequence[str]]): Optional sequence of names of
          columns to include in the generated data itemes. Any columns not in the
          sequence are excluded. If not provided, all columns are included.
        slot_name (str): Name of the replication slot to consume replication
          messages from. Each table is expected to have a dedicated slot.
        publication_name (str): Name of the publication that published DML operations
          for the table. Each table is expected to have a dedicated publication.
        upto_lsn Optional[int]: Optional integer LSN value upto which the replication
          slot is consumed. If not provided, all messages in the slot are consumed,
          ensuring all new changes in the source table are included.

        Returns:
            DltResource that yields data items for changes in the postgres table.
    """
    return dlt.resource(
        table_replication_items,
        name=table,
        write_disposition="merge",
        primary_key=primary_key,
        columns={"lsn": {"dedup_sort": "desc"}},
    )(
        credentials=credentials,
        slot_name=slot_name,
        publication_name=publication_name,
        include_columns=include_columns,
        upto_lsn=upto_lsn,
    )
