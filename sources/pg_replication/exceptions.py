from typing import Sequence


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
