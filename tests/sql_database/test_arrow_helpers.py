from datetime import datetime, timezone, date
from uuid import uuid4

import pytest
import pyarrow as pa

from sources.sql_database.arrow_helpers import row_tuples_to_arrow


@pytest.mark.parametrize("all_unknown", [True, False])
def test_row_tuples_to_arrow_unknown_types(all_unknown: bool) -> None:
    """Test inferring data types with pyarrow"""
    rows = [
        (
            1,
            "a",
            1.1,
            True,
            date.today(),
            uuid4(),
            datetime.now(timezone.utc),
            [1, 2, 3],
        ),
        (
            2,
            "b",
            2.2,
            False,
            date.today(),
            uuid4(),
            datetime.now(timezone.utc),
            [4, 5, 6],
        ),
        (
            3,
            "c",
            3.3,
            True,
            date.today(),
            uuid4(),
            datetime.now(timezone.utc),
            [7, 8, 9],
        ),
    ]

    # Some columns don't specify data type and should be inferred
    columns = {
        "int_col": {"name": "int_col", "data_type": "bigint", "nullable": False},
        "str_col": {"name": "str_col", "data_type": "text", "nullable": False},
        "float_col": {"name": "float_col", "nullable": False},
        "bool_col": {"name": "bool_col", "data_type": "bool", "nullable": False},
        "date_col": {"name": "date_col", "nullable": False},
        "uuid_col": {"name": "uuid_col", "nullable": False},
        "datetime_col": {
            "name": "datetime_col",
            "data_type": "timestamp",
            "nullable": False,
        },
        "array_col": {"name": "array_col", "nullable": False},
    }

    if all_unknown:
        for col in columns.values():
            col.pop("data_type", None)

    # Call the function

    result = row_tuples_to_arrow(rows, columns, tz="UTC")  # type: ignore[arg-type]

    # Result is arrow table containing all columns in original order with correct types
    assert result.num_columns == len(columns) - 1
    result_col_names = [f.name for f in result.schema]
    # UUID object is not supported by arrow
    expected_names = [c for c in columns if c != "uuid_col"]
    assert result_col_names == expected_names

    assert pa.types.is_int64(result[0].type)
    assert pa.types.is_string(result[1].type)
    assert pa.types.is_float64(result[2].type)
    assert pa.types.is_boolean(result[3].type)
    assert pa.types.is_date(result[4].type)
    assert pa.types.is_timestamp(result[5].type)
    assert pa.types.is_list(result[6].type)
