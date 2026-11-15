from copy import deepcopy
from typing import Any, Dict, List, Literal, Optional

import dlt
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.exceptions import DltException
from dlt.common.schema.typing import TColumnSchema, TColumnType, TTableSchemaColumns
from dlt.common.schema.utils import merge_columns
from dlt.common.typing import TDataItem
from dlt.extract.hints import TResourceHints
from dlt.extract.items import DataItemWithMeta, TableNameMeta
from dlt.extract.resource import DltResource

TOnTypeChange = Literal["strict", "skip"]
COLUMN_TYPE_HINTS = ("data_type", "precision", "scale", "timezone")


class IncompatibleColumnTypeException(DltException):
    def __init__(
        self,
        table_name: str,
        column_name: str,
        destination_type: str,
        new_type: str,
        new_column: Dict[str, Any],
    ) -> None:
        self.table_name = table_name
        self.column_name = column_name
        self.destination_type = destination_type
        self.new_type = new_type
        self.columns = {column_name: new_column}
        """`columns` to apply to the resource to load the column with the new type"""
        super().__init__(
            f"The type of column `{column_name}` of table `{table_name}` changed to {new_type}"
            f" in a schema of the Schema Registry, which does not fit the destination column of"
            f" type {destination_type}. The run stopped before loading messages of that schema,"
            " their offsets are not stored. To load them, change the type of the destination"
            " column yourself and apply `columns="
            f"{self.columns!r}` to the resource. Or drop the table with"
            ' `refresh="drop_resources"`, which also resets the stored offsets. Or pass'
            ' `on_type_change="skip"` to `kafka_consumer` and decide with the `data_type`'
            " schema contract."
        )


def column_type_fits(existing: TColumnSchema, new: TColumnSchema) -> bool:
    """Tells if every value of the `new` column type fits a destination column of the
    `existing` type. A decimal without precision has the default precision of dlt."""
    data_type = existing.get("data_type")
    if data_type != new.get("data_type"):
        return False
    if data_type == "decimal":
        precision = existing.get("precision", DEFAULT_NUMERIC_PRECISION)
        scale = existing.get("scale", DEFAULT_NUMERIC_SCALE)
        new_precision = new.get("precision", DEFAULT_NUMERIC_PRECISION)
        new_scale = new.get("scale", DEFAULT_NUMERIC_SCALE)
        return new_scale <= scale and new_precision - new_scale <= precision - scale
    if data_type == "timestamp" and existing.get("timezone", True) != new.get(
        "timezone", True
    ):
        return False
    precision = existing.get("precision")
    if precision is None:
        return True
    return new.get("precision") is not None and new["precision"] <= precision


def format_column_type(column: TColumnType) -> str:
    """Formats a dlt column type for messages, ie. `decimal(10,2)`"""
    params = [str(column[h]) for h in ("precision", "scale") if column.get(h) is not None]  # type: ignore[literal-required]
    formatted = column["data_type"] + (f"({','.join(params)})" if params else "")
    if column["data_type"] == "timestamp" and column.get("timezone") is False:
        formatted += " without time zone"
    return formatted


class TableHints:
    """Columns of the tables of a resource: the schema columns of a table merged with the columns
    the user applied to the resource, which win"""

    def __init__(
        self, resource: Optional[DltResource], on_type_change: TOnTypeChange = "strict"
    ) -> None:
        self.resource = resource
        self.on_type_change = on_type_change
        self._user_columns: Dict[str, TTableSchemaColumns] = {}
        self._schema_columns: Dict[str, TTableSchemaColumns] = {}
        self._columns: Dict[str, TTableSchemaColumns] = {}
        self._pending: Dict[str, TResourceHints] = {}

    def user_columns(self, table: str) -> TTableSchemaColumns:
        """Columns applied to the resource or to its `table` variant, read before the first
        emitted hints of `table`"""
        if table not in self._user_columns:
            columns: TTableSchemaColumns = {}
            resource = self.resource
            if (
                resource is not None
                and not resource.has_dynamic_table_name
                and not resource.has_other_dynamic_hints
            ):
                computed = resource.compute_table_schema(meta=TableNameMeta(table))
                columns = deepcopy(computed.get("columns", {}))
            self._user_columns[table] = columns
        return self._user_columns[table]

    def columns(self, table: str) -> Optional[TTableSchemaColumns]:
        """Emitted columns of `table`, or the user columns when none were emitted"""
        if table in self._columns:
            return self._columns[table]
        return self.user_columns(table) or None

    def update(self, table: str, columns: Optional[TTableSchemaColumns]) -> None:
        """Keeps the hints of `table` for its next data item when schema `columns` add a column
        or change one.

        The columns of all schemas seen for `table` are kept, so messages of older and newer
        schemas that interleave do not emit hints at every switch.

        Raises:
            IncompatibleColumnTypeException: In "strict" mode, when a column loaded before gets
                a type whose values do not fit it.
        """
        if columns is None:
            return
        seen = self._schema_columns.get(table)
        union = (
            deepcopy(columns)
            if seen is None
            else merge_columns(deepcopy(seen), deepcopy(columns))
        )
        if union == seen:
            return
        user_columns = self.user_columns(table)
        if self.on_type_change == "strict":
            self._keep_loaded_types(table, union, user_columns)
        self._schema_columns[table] = union
        merged = merge_columns(deepcopy(union), deepcopy(user_columns))
        self._columns[table] = merged
        self._pending[table] = dlt.mark.make_hints(table_name=table, columns=merged)

    def with_hints(self, table: str, items: List[TDataItem]) -> List[TDataItem]:
        """`items` of `table`, the first one carries the hints of `table` kept by `update`.
        Hints ride on data: dlt before 1.32 counts empty items in `add_limit`."""
        if not items or table not in self._pending:
            return items
        first = items[0]
        data = first.data if isinstance(first, DataItemWithMeta) else first
        hints = self._pending.pop(table)
        return [
            dlt.mark.with_hints(data, hints, create_table_variant=True),
            *items[1:],
        ]

    def _keep_loaded_types(
        self,
        table: str,
        columns: TTableSchemaColumns,
        user_columns: TTableSchemaColumns,
    ) -> None:
        """Keeps the types of the columns loaded before in `columns` when the new types fit them,
        raises `IncompatibleColumnTypeException` when they do not"""
        for name, column in columns.items():
            if "data_type" in user_columns.get(name, {}):
                continue
            existing = self._loaded_column(table, name)
            if existing is None or "data_type" not in existing:
                continue
            if not column_type_fits(existing, column):
                raise IncompatibleColumnTypeException(
                    table,
                    name,
                    format_column_type(existing),
                    format_column_type(column),
                    {h: column[h] for h in COLUMN_TYPE_HINTS if h in column},  # type: ignore[literal-required]
                )
            schema: Dict[str, Any] = column  # type: ignore[assignment]
            for hint in COLUMN_TYPE_HINTS:
                schema.pop(hint, None)
                if hint in existing:
                    schema[hint] = existing[hint]  # type: ignore[literal-required]

    def _loaded_column(self, table: str, name: str) -> Optional[TColumnSchema]:
        """The column emitted earlier in this run or loaded by an earlier run"""
        if name in self._columns.get(table, {}):
            return self._columns[table][name]
        try:
            schema = dlt.current.source_schema()
        except DltException:
            return None
        loaded = schema.tables.get(schema.naming.normalize_table_identifier(table))
        if loaded is None:
            return None
        return loaded["columns"].get(schema.naming.normalize_path(name))
