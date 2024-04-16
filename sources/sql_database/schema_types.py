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


def sqla_col_to_column_schema(
    sql_col: ColumnAny, add_precision: bool = False
) -> Optional[TColumnSchema]:
    """Infer dlt schema column type from an sqlalchemy type.

    If `add_precision` is set, precision and scale is inferred from that types that support it,
    such as numeric, varchar, int, bigint. Numeric (decimal) types have always precision added.
    """
    sql_t = sql_col.type
    col: TColumnSchema = {
        "name": sql_col.name,
        "data_type": None,  # set that later
        "nullable": sql_col.nullable,
    }

    if isinstance(sql_t, sqltypes.SmallInteger):
        col["data_type"] = "bigint"
        if add_precision:
            col["precision"] = 32
    elif isinstance(sql_t, sqltypes.Integer):
        col["data_type"] = "bigint"
    elif isinstance(sql_t, sqltypes.Numeric):
        if isinstance(sql_t, sqltypes.Float):
            col["data_type"] = "double"
        else:
            col["data_type"] = "decimal"
            # always emit precision for decimals
            col["precision"] = sql_t.precision
            col["scale"] = sql_t.scale
    elif isinstance(sql_t, sqltypes.String):
        col["data_type"] = "text"
        if add_precision and sql_t.length:
            col["precision"] = sql_t.length
    elif isinstance(sql_t, sqltypes._Binary):
        col["data_type"] = "binary"
        if add_precision and sql_t.length:
            col["precision"] = sql_t.length
    elif isinstance(sql_t, sqltypes.DateTime):
        col["data_type"] = "timestamp"
    elif isinstance(sql_t, sqltypes.Date):
        col["data_type"] = "date"
    elif isinstance(sql_t, sqltypes.Time):
        col["data_type"] = "time"
    elif isinstance(sql_t, sqltypes.JSON):
        col["data_type"] = "complex"
    elif isinstance(sql_t, sqltypes.Boolean):
        col["data_type"] = "bool"
    else:
        logger.warning(
            f"A column with name {sql_col.name} contains unknown data type {sql_t} which cannot be mapped to `dlt` data type. When using sqlalchemy backend such data will be passed to the normalizer. In case of `pyarrow` backend such data will be ignored. In case of other backends, the behavior is backend-specific."
        )
        col = None
    if col:
        return {key: value for key, value in col.items() if value is not None}  # type: ignore[return-value]
    return None


def table_to_columns(table: Table, add_precision: bool = False) -> TTableSchemaColumns:
    """Convert an sqlalchemy table to a dlt table schema.

    Adds precision to columns when `add_precision` is set.
    """
    return {
        col["name"]: col
        for col in (sqla_col_to_column_schema(c, add_precision) for c in table.columns)
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
