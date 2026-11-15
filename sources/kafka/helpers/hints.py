from copy import deepcopy
from typing import Dict, Optional

import dlt
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.schema.utils import merge_columns
from dlt.common.typing import TDataItem
from dlt.extract.items import TableNameMeta
from dlt.extract.resource import DltResource


class TableHints:
    """Columns of the tables of a resource: the schema columns of a table merged with the columns
    the user applied to the resource, which win"""

    def __init__(self, resource: Optional[DltResource]) -> None:
        self.resource = resource
        self._user_columns: Dict[str, TTableSchemaColumns] = {}
        self._schema_columns: Dict[str, TTableSchemaColumns] = {}
        self._columns: Dict[str, TTableSchemaColumns] = {}

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

    def update(
        self, table: str, columns: Optional[TTableSchemaColumns]
    ) -> Optional[TDataItem]:
        """Hints of `table` when schema `columns` add a column or change one, `None` otherwise.

        The columns of all schemas seen for `table` are kept, so messages of older and newer
        schemas that interleave do not emit hints at every switch.
        """
        if columns is None:
            return None
        seen = self._schema_columns.get(table)
        union = (
            deepcopy(columns)
            if seen is None
            else merge_columns(deepcopy(seen), deepcopy(columns))
        )
        if union == seen:
            return None
        user_columns = self.user_columns(table)
        self._schema_columns[table] = union
        merged = merge_columns(deepcopy(union), deepcopy(user_columns))
        self._columns[table] = merged
        return dlt.mark.with_hints(
            [],
            dlt.mark.make_hints(table_name=table, columns=merged),
            create_table_variant=True,
        )
