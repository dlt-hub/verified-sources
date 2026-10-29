from typing import Any, Dict, NamedTuple, Optional, Sequence


def format_lsn(lsn: int) -> str:
    """Formats an LSN as Postgres does, ie. `16/B374D848`."""
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


class ChangePosition(NamedTuple):
    """Transactions with a change that stops the replication, by the LSN of their commit."""

    slot_name: str
    first_final_lsn: int
    last_final_lsn: int

    @property
    def skip_lsn(self) -> str:
        """The position to advance the slot to so the transactions are skipped."""
        # a slot skips transactions committed before its position, not at it
        return format_lsn(self.last_final_lsn + 1)

    def __str__(self) -> str:
        if self.first_final_lsn == self.last_final_lsn:
            return f"the transaction committed at LSN {format_lsn(self.last_final_lsn)}"
        return (
            "one of the transactions committed at LSN"
            f" {format_lsn(self.first_final_lsn)} to {format_lsn(self.last_final_lsn)}"
        )

    def skip_hint(self) -> str:
        return (
            f" The change is in {self}. To skip it, run `SELECT pg_replication_slot_advance("
            f"'{self.slot_name}', '{self.skip_lsn}');` (Postgres 11+) while no replication runs."
            " This also skips all changes before it that were not loaded yet."
        )


def _changed_in(position: Optional[ChangePosition]) -> str:
    return "" if position is None else f" The table changed in {position}."


class NoPrimaryKeyException(Exception):
    def __init__(
        self,
        schema_name: str,
        table_names: Sequence[str],
        position: Optional[ChangePosition] = None,
    ) -> None:
        self.schema_name = schema_name
        self.table_names = table_names
        self.position = position
        qual_names = ", ".join(f"{schema_name}.{name}" for name in table_names)
        super().__init__(
            f"Tables {qual_names} have no primary key or replica identity index and the"
            " publication publishes updates or deletes: Postgres rejects their updates and"
            " deletes, with REPLICA IDENTITY FULL their changes cannot be merged. Add a primary"
            " key, set REPLICA IDENTITY USING INDEX or publish only inserts with"
            ' `publish="insert"`, then initialize the replication again with `reset=True` and an'
            " initial snapshot." + _changed_in(position)
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
        position: Optional[ChangePosition] = None,
    ) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_name = column_name
        self.destination_type = destination_type
        self.new_type = new_type
        self.position = position
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
            " the column with `exclude_columns`." + _changed_in(position)
        )


class PublicationPrivilegesException(Exception):
    def __init__(self, pub_name: str, statement: str) -> None:
        self.pub_name = pub_name
        self.statement = statement
        super().__init__(
            f"The user cannot set up publication {pub_name}: creating a publication needs the"
            " CREATE privilege on the database, adding tables needs their ownership and adding a"
            " schema a superuser. Ask the table owner or a superuser to run"
            f" `{statement}`, then call `init_replication` again. The replication itself needs"
            " only the REPLICATION attribute and SELECT on the tables."
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
        self,
        schema_name: str,
        table_name: str,
        column_names: Sequence[str],
        position: Optional[ChangePosition] = None,
        missing: bool = False,
    ) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_names = column_names
        self.position = position
        reason = (
            "do not have them (renamed, dropped or excluded columns)"
            if missing
            else "have NULL values for them"
        )
        hint = ""
        if position is not None:
            hint = _changed_in(position) if missing else position.skip_hint()
        super().__init__(
            f"Columns {', '.join(column_names)} of table {schema_name}.{table_name} are NOT NULL"
            f" in the destination, but the replicated changes {reason}. The replication stopped"
            " before loading the changes. Drop NOT NULL of the columns in the destination"
            " yourself and pass `nullable: True` for them in `columns`, or initialize the"
            " replication again with `reset=True` and an initial snapshot." + hint
        )


class UnchangedToastException(Exception):
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        column_names: Sequence[str],
        position: Optional[ChangePosition] = None,
    ) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_names = column_names
        self.position = position
        super().__init__(
            f"An update of table {schema_name}.{table_name} did not send the unchanged large"
            f" (TOASTed) values of columns {', '.join(column_names)}, because the table does not"
            f" have REPLICA IDENTITY FULL. Run `ALTER TABLE {schema_name}.{table_name} REPLICA"
            " IDENTITY FULL` or exclude the columns with `exclude_columns`, then initialize the"
            " replication again with `reset=True` and an initial snapshot: the update stays in"
            " the replication slot until then."
            + ("" if position is None else position.skip_hint())
        )


class ValueConversionException(Exception):
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        column_name: Optional[str],
        error: str,
        position: ChangePosition,
    ) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_name = column_name
        self.position = position
        where = "" if column_name is None else f" of column {column_name}"
        super().__init__(
            f"A value{where} of table {schema_name}.{table_name} cannot be loaded: {error}."
            " Stop replicating the column with `exclude_columns`, or skip the change."
            + position.skip_hint()
        )
