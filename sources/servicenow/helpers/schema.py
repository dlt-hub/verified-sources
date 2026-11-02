"""Maps `sys_dictionary` fields to dlt columns and picks the cursor and write disposition"""

from typing import Any, Dict, List, NamedTuple, Optional, Set

from dlt.common.schema.typing import (
    TColumnSchema,
    TTableSchemaColumns,
    TWriteDisposition,
)

PRIMARY_KEY = "sys_id"
UPDATED_AT_FIELD = "sys_updated_on"
CREATED_AT_FIELD = "sys_created_on"
# `hard_delete` column of records deleted in ServiceNow
DELETED_COLUMN = "is_deleted"
# the instance can commit a record with a timestamp older than the newest one already read
DEFAULT_LAG = 600.0

# journal entries are records of `sys_journal_field`, passwords cannot be read
SKIPPED_TYPES = {"journal", "journal_input", "password", "password2"}
# durations are date-times after 1970-01-01 00:00:00, loaded as seconds
DURATION_TYPES = {"glide_duration", "timer"}


class TableColumns(NamedTuple):
    columns: TTableSchemaColumns
    durations: Set[str]


def internal_type_to_column(
    element: str, internal_type: str
) -> Optional[TColumnSchema]:
    """Converts a `sys_dictionary` field into a dlt column schema, None for skipped types.

    Types not mapped below, including references, choices and types added in the future, are
    loaded as text.
    """
    if internal_type in SKIPPED_TYPES:
        return None
    col: TColumnSchema = {"name": element}
    if internal_type == "boolean":
        col["data_type"] = "bool"
    elif internal_type in ("integer", "longint") or internal_type in DURATION_TYPES:
        col["data_type"] = "bigint"
    elif internal_type in ("decimal", "currency", "price"):
        col["data_type"] = "decimal"
    elif internal_type == "float":
        col["data_type"] = "double"
    elif internal_type in ("glide_date_time", "due_date"):
        col["data_type"] = "timestamp"
        col["timezone"] = True
    elif internal_type == "glide_date":
        col["data_type"] = "date"
    elif internal_type == "glide_time":
        col["data_type"] = "time"
        col["precision"] = 0
    else:
        col["data_type"] = "text"
    return col


def table_chain(tables: Dict[str, str], table: str) -> List[str]:
    """Returns `table` followed by its parent tables, `tables` maps names to parent names"""
    if table not in tables:
        raise ValueError(
            f"Table `{table}` is not in `sys_db_object`. The table does not exist, is a database"
            " view, or the user cannot read `sys_db_object`."
        )
    chain = [table]
    while tables.get(chain[-1]) and tables[chain[-1]] not in chain:
        chain.append(tables[chain[-1]])
    return chain


def descendant_tables(tables: Dict[str, str], table: str) -> List[str]:
    """Returns tables that extend `table` directly or through other tables"""
    descendants: List[str] = []
    parents = [table]
    while parents:
        children = [name for name, parent in tables.items() if parent in parents]
        children = [
            name for name in children if name not in descendants and name != table
        ]
        descendants.extend(children)
        parents = children
    return descendants


def dictionary_to_columns(
    dictionary: List[Dict[str, Any]],
    chain: List[str],
    included_columns: Optional[List[str]] = None,
    excluded_columns: Optional[List[str]] = None,
) -> TableColumns:
    """Converts the `sys_dictionary` fields of the tables in `chain` into columns of its first
    table. Fields of a table replace the fields it inherits.
    """
    fields: Dict[str, str] = {}
    for table in reversed(chain):
        for entry in dictionary:
            if entry["name"] == table:
                fields[entry["element"]] = entry["internal_type"]
    if included_columns:
        fields = {
            name: type_ for name, type_ in fields.items() if name in included_columns
        }
    if excluded_columns:
        fields = {
            name: type_
            for name, type_ in fields.items()
            if name not in excluded_columns
        }
    columns: TTableSchemaColumns = {}
    for element, internal_type in fields.items():
        col = internal_type_to_column(element, internal_type)
        if col is not None:
            columns[element] = col
    durations = {name for name, type_ in fields.items() if type_ in DURATION_TYPES}
    return TableColumns(columns, durations)


def get_cursor_field(columns: TTableSchemaColumns) -> Optional[str]:
    """Returns `sys_updated_on`, or `sys_created_on` if the table has no `sys_updated_on`.
    Returns None if the table has neither field.
    """
    for name in (UPDATED_AT_FIELD, CREATED_AT_FIELD):
        if name in columns:
            return name
    return None


def get_write_disposition(cursor_field: Optional[str]) -> TWriteDisposition:
    """Returns `replace` without a cursor, `append` for the `sys_created_on` cursor and `merge`
    for other cursors
    """
    if cursor_field is None:
        return "replace"
    if cursor_field == CREATED_AT_FIELD:
        return "append"
    return "merge"
