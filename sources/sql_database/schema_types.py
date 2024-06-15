from typing import (
    Optional,
    Any,
    Sequence,
    Type,
    TYPE_CHECKING,
    Literal,
    List,
    Callable,
    Union,
)
from typing_extensions import TypeAlias
from sqlalchemy import Table, Column
from sqlalchemy.engine import Row
from sqlalchemy.sql import sqltypes, Select

from dlt.common import logger
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns, TDataType
from dlt.common.configuration import with_config
from dlt.common.destination import DestinationCapabilitiesContext

ReflectionLevel = Literal["minimal", "full", "full_with_precision"]


# optionally create generics with any so they can be imported by dlt importer
if TYPE_CHECKING:
    SelectAny: TypeAlias = Select[Any]
    ColumnAny: TypeAlias = Column[Any]
    RowAny: TypeAlias = Row[Any]
else:
    SelectAny: TypeAlias = Type[Any]
    ColumnAny: TypeAlias = Type[Any]
    RowAny: TypeAlias = Type[Any]


TTypeAdapter = Callable[
    [sqltypes.TypeEngine[Any]],
    Optional[Union[sqltypes.TypeEngine[Any], Type[sqltypes.TypeEngine[Any]]]],
]


def sqla_col_to_column_schema(
    sql_col: ColumnAny,
    reflection_level: ReflectionLevel,
    type_adapter_callback: Optional[TTypeAdapter] = None,
) -> Optional[TColumnSchema]:
    """Infer dlt schema column type from an sqlalchemy type.

    If `add_precision` is set, precision and scale is inferred from that types that support it,
    such as numeric, varchar, int, bigint. Numeric (decimal) types have always precision added.
    """
    col: TColumnSchema = {
        "name": sql_col.name,
        "nullable": sql_col.nullable,
    }
    if reflection_level == "minimal":
        return col

    sql_t = sql_col.type

    sql_t = type_adapter_callback(sql_t) if type_adapter_callback else sql_t
    if type_adapter_callback:
        sql_t = type_adapter_callback(sql_t)
        # Check if sqla type class rather than instance is returned
        if sql_t is not None and isinstance(sql_t, type):
            sql_t = sql_t()

    if sql_t is None:
        # Column ignored by callback
        return col

    add_precision = reflection_level == "full_with_precision"

    if isinstance(sql_t, sqltypes.SmallInteger):
        col["data_type"] = "bigint"
        if add_precision:
            col["precision"] = 32
    elif isinstance(sql_t, sqltypes.Integer):
        col["data_type"] = "bigint"
    elif isinstance(sql_t, sqltypes.Numeric):
        # dlt column type depends on the data returned by the sql alchemy dialect
        # and not on the metadata reflected in the database. all Numeric types
        # that are returned as floats will assume "double" type
        # and returned as decimals will assume "decimal" type
        if sql_t.asdecimal is False:
            col["data_type"] = "double"
        else:
            col["data_type"] = "decimal"
            if sql_t.precision is not None:
                col["precision"] = sql_t.precision
                # must have a precision for any meaningful scale
                if sql_t.scale is not None:
                    col["scale"] = sql_t.scale
                elif sql_t.decimal_return_scale is not None:
                    col["scale"] = sql_t.decimal_return_scale
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

    return {key: value for key, value in col.items() if value is not None}  # type: ignore[return-value]


def get_primary_key(table: Table) -> Optional[List[str]]:
    """Create primary key or return None if no key defined"""
    primary_key = [c.name for c in table.primary_key]
    return primary_key if len(primary_key) > 0 else None


def table_to_columns(
    table: Table,
    reflection_level: ReflectionLevel = "full",
    type_conversion_fallback: Optional[TTypeAdapter] = None,
) -> TTableSchemaColumns:
    """Convert an sqlalchemy table to a dlt table schema."""
    return {
        col["name"]: col
        for col in (
            sqla_col_to_column_schema(c, reflection_level, type_conversion_fallback)
            for c in table.columns
        )
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
            if schema_item.get("data_type") is not None
        ]
    )


def row_tuples_to_arrow(
    rows: Sequence[RowAny], columns: TTableSchemaColumns, tz: str
) -> Any:
    from dlt.common.libs.pyarrow import pyarrow as pa
    import numpy as np

    arrow_schema = columns_to_arrow(columns, tz=tz)

    columns_list = list(columns.values())
    included_columns_idx = [
        idx for idx, col in enumerate(columns_list) if col.get("data_type")
    ]
    included_column_names = [columns_list[idx]["name"] for idx in included_columns_idx]

    try:
        from pandas._libs import lib

        pivoted_rows = lib.to_object_array_tuples(rows).T  # type: ignore[attr-defined]
    except ImportError:
        logger.info(
            "Pandas not installed, reverting to numpy.asarray to create a table which is slower"
        )
        pivoted_rows = np.asarray(rows, dtype="object", order="k").T  # type: ignore[call-overload]

    columnar = {
        col: dat.ravel()
        for col, dat in zip(
            included_column_names,
            np.vsplit(pivoted_rows[included_columns_idx], len(included_columns_idx)),
        )
    }
    for idx in range(0, len(arrow_schema.names)):
        field = arrow_schema.field(idx)
        py_type = type(rows[0][idx])
        # cast double / float ndarrays to decimals if type mismatch, looks like decimals and floats are often mixed up in dialects
        if pa.types.is_decimal(field.type) and issubclass(py_type, (str, float)):
            logger.warning(
                f"Field {field.name} was reflected as decimal type, but rows contains {py_type.__name__}. Additional cast is required which may slow down arrow table generation."
            )
            float_array = pa.array(columnar[field.name], type=pa.float64())
            columnar[field.name] = float_array.cast(field.type, safe=False)
    return pa.Table.from_pydict(columnar)
