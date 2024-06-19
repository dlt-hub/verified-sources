from typing import Any, Sequence

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common import logger
from dlt.common.configuration import with_config
from dlt.common.destination import DestinationCapabilitiesContext

from .schema_types import RowAny


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
    return pa.Table.from_pydict(columnar, schema=arrow_schema)
