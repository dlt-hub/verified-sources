from typing import Optional, Any, Type, TYPE_CHECKING
from typing_extensions import TypeAlias
from sqlalchemy import Table, Column
from sqlalchemy.sql import sqltypes, Select

from dlt.common import logger
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.common.configuration import with_config
from dlt.common.destination import DestinationCapabilitiesContext

# optionally create generics with any so they can be imported by dlt importer
if TYPE_CHECKING:
    SelectAny: TypeAlias = Select[Any]
    ColumnAny: TypeAlias = Column[Any]
else:
    SelectAny: TypeAlias = Type[Any]
    ColumnAny: TypeAlias = Type[Any]


def sqla_col_to_column_schema(sql_col: ColumnAny) -> Optional[TColumnSchema]:
    """Infer dlt schema column type from an sqlalchemy type.

    Precision and scale is inferred from that types that support it,
    such as numeric, varchar, int, bigint
    """
    sql_t = sql_col.type
    col = None

    if isinstance(sql_t, sqltypes.BigInteger):
        col = dict(
            name=sql_col.name,
            data_type="bigint",
            precision=64,
            nullable=sql_col.nullable,
        )
    elif isinstance(sql_t, sqltypes.SmallInteger):
        col = dict(
            name=sql_col.name,
            data_type="bigint",
            precision=16,
            nullable=sql_col.nullable,
        )
    elif isinstance(sql_t, sqltypes.Integer):
        col = dict(
            name=sql_col.name,
            data_type="bigint",
            precision=32,
            nullable=sql_col.nullable,
        )
    elif isinstance(sql_t, sqltypes.Numeric):
        if isinstance(sql_t, sqltypes.Float):
            col = dict(name=sql_col.name, data_type="double", nullable=sql_col.nullable)
        else:
            col = dict(
                name=sql_col.name,
                data_type="decimal",
                nullable=sql_col.nullable,
                precision=sql_t.precision,
                scale=sql_t.scale,
            )
    elif isinstance(sql_t, sqltypes.String):
        col = dict(
            name=sql_col.name,
            data_type="text",
            precision=sql_t.length,
            nullable=sql_col.nullable,
        )
    elif isinstance(sql_t, sqltypes._Binary):
        col = dict(
            name=sql_col.name,
            data_type="binary",
            precision=sql_t.length,
            nullable=sql_col.nullable,
        )
    elif isinstance(sql_t, sqltypes.DateTime):
        col = dict(name=sql_col.name, data_type="timestamp", nullable=sql_col.nullable)
    elif isinstance(sql_t, sqltypes.Date):
        col = dict(name=sql_col.name, data_type="date", nullable=sql_col.nullable)
    elif isinstance(sql_t, sqltypes.Time):
        col = dict(name=sql_col.name, data_type="time", nullable=sql_col.nullable)
    elif isinstance(sql_t, sqltypes.JSON):
        col = dict(name=sql_col.name, data_type="complex", nullable=sql_col.nullable)
    elif isinstance(sql_t, sqltypes.Boolean):
        col = dict(name=sql_col.name, data_type="bool", nullable=sql_col.nullable)
    else:
        logger.warning(
            f"A column with name {sql_col.name} contains unknown data type {sql_t} which cannot be mapped to `dlt` data type. When using sqlalchemy backend such data will be passed to the normalizer. In case of `pyarrow` backend such data will be ignored. In case of other backends, the behavior is backend-specific."
        )
    if col:
        return {key: value for key, value in col.items() if value is not None}  # type: ignore[return-value]
    return None


def table_to_columns(table: Table) -> TTableSchemaColumns:
    """Convert an sqlalchemy table to a dlt table schema.

    Only columns types supporting precision/scale are included in result.
    """
    return {
        col["name"]: col
        for col in (sqla_col_to_column_schema(c) for c in table.columns)
        if col is not None
    }


@with_config
def columns_to_arrow(
    columns_schema: TTableSchemaColumns,
    caps: DestinationCapabilitiesContext = None,
    tz: str = "UTC",
) -> Any:
    """Converts `column_schema` to arrow schema using `caps` and `tz`. `caps` are injected from the container - which
    is always the case if run within the pipeline. This will generate arrow schema compatible with the destination.
    Otherwise generic capabilities are used
    """
    from dlt.common.libs.pyarrow import pyarrow as pa, get_py_arrow_datatype
    from dlt.common.destination.capabilities import DestinationCapabilitiesContext

    return pa.schema(
        [
            pa.field(
                name,
                get_py_arrow_datatype(
                    schema_item,
                    caps or DestinationCapabilitiesContext.generic_capabilities(),
                    tz,
                ),
                nullable=schema_item.get("nullable", True),
            )
            for name, schema_item in columns_schema.items()
        ]
    )
