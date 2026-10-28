from typing import Any, Dict, Sequence


class NoPrimaryKeyException(Exception):
    def __init__(self, schema_name: str, table_names: Sequence[str]) -> None:
        self.schema_name = schema_name
        self.table_names = table_names
        qual_names = ", ".join(f"{schema_name}.{name}" for name in table_names)
        super().__init__(
            f"Tables {qual_names} have no primary key or replica identity index and the"
            " publication publishes updates or deletes: Postgres rejects their updates and"
            " deletes, with REPLICA IDENTITY FULL their changes cannot be merged. Add a primary"
            " key, set REPLICA IDENTITY USING INDEX or publish only inserts with"
            ' `publish="insert"`, then initialize the replication again with `reset=True` and an'
            " initial snapshot."
        )


class IncompatiblePostgresVersionException(Exception):
    pass


class IncompatibleColumnTypeException(Exception):
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        destination_type: str,
        new_type: str,
        new_column_type: Dict[str, Any],
    ) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_name = column_name
        self.destination_type = destination_type
        self.new_type = new_type
        self.columns = {table_name: {column_name: new_column_type}}
        """`columns` of `replication_resource` that load the column with the new type."""
        super().__init__(
            f"The type of column {column_name} of table {schema_name}.{table_name} changed to"
            f" {new_type}, which does not fit the destination column of type {destination_type}."
            " The replication stopped before loading the change. To load it, change the type of"
            f" the destination column to {new_type} yourself, drop the table {table_name} from the"
            " staging dataset (merge loads there first, dlt creates it again) and pass"
            f" `columns={self.columns!r}` to `replication_resource`. Or initialize the"
            " replication again with `reset=True` and an initial snapshot, or stop replicating"
            " the column with `exclude_columns`."
        )


class SnapshotSchemaNotFoundException(Exception):
    def __init__(self, pipeline_name: str) -> None:
        self.pipeline_name = pipeline_name
        super().__init__(
            f"Pipeline {pipeline_name} passed as `snapshot_pipeline` has no schema. Restore it"
            " from the destination with `sync_destination()` of the pipeline before the first"
            " replication run."
        )


class NullInNotNullColumnException(Exception):
    def __init__(
        self, schema_name: str, table_name: str, column_names: Sequence[str]
    ) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_names = column_names
        super().__init__(
            f"Columns {', '.join(column_names)} of table {schema_name}.{table_name} are NOT NULL"
            " in the destination, but the replicated changes have NULL values for them or do not"
            " have them (renamed, dropped or excluded columns). The replication stopped before"
            " loading the changes. Drop NOT NULL of the columns in the destination yourself and"
            " pass `nullable: True` for them in `columns`, or initialize the replication again"
            " with `reset=True` and an initial snapshot."
        )


class UnchangedToastException(Exception):
    def __init__(
        self, schema_name: str, table_name: str, column_names: Sequence[str]
    ) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_names = column_names
        super().__init__(
            f"An update of table {schema_name}.{table_name} did not send the unchanged large"
            f" (TOASTed) values of columns {', '.join(column_names)}, because the table does not"
            f" have REPLICA IDENTITY FULL. Run `ALTER TABLE {schema_name}.{table_name} REPLICA"
            " IDENTITY FULL` or exclude the columns with `exclude_columns`, then initialize the"
            " replication again with `reset=True` and an initial snapshot: the update stays in"
            " the replication slot until then."
        )
