from collections import Counter
from datetime import datetime, timedelta  # noqa: I251
from typing import Any, Callable, Dict, Iterable, List, Tuple

import dlt

from tests.utils import load_tables_to_dicts


def seeded_windows(
    values: List[datetime], unit: timedelta
) -> List[Tuple[Callable[..., Any], List[datetime]]]:
    """Window bounds at the quartiles of the seeded `values`, forward with `max` and back in time
    with `min`. Bounds are seeded values, so a value on a bound loads in exactly one window. The
    outer bounds are one cursor `unit` past the seeded values
    """
    values = sorted(values)
    quarters = [values[len(values) * k // 4] for k in (1, 2, 3)]
    return [
        (max, [values[0], *quarters, values[-1] + unit]),
        (min, [values[-1], *quarters[::-1], values[0] - unit]),
    ]


def in_window(
    value: datetime, start: datetime, end: datetime, last_value_func: Any
) -> bool:
    return start <= value < end if last_value_func is max else end < value <= start


def assert_loaded_rows(
    pipeline: dlt.Pipeline,
    table: str,
    number_column: str,
    expected_row: Callable[[int], Dict[str, Any]],
    numbers: Iterable[int],
) -> None:
    """Checks that each of `numbers` loaded once to `table` and compares every row with the
    `expected_row` of the number in its `number_column`
    """
    naming = pipeline.default_schema.naming
    table_name = naming.normalize_path(table)
    number_name = naming.normalize_path(number_column)
    rows = load_tables_to_dicts(pipeline, table_name)[table_name]
    counts = Counter(int(row[number_name]) for row in rows)
    expected = set(numbers)
    assert [n for n, count in counts.items() if count > 1] == [], "duplicate rows"
    assert sorted(expected - counts.keys())[:10] == [], "missing rows"
    assert sorted(counts.keys() - expected)[:10] == [], "unexpected rows"
    for row in rows:
        n = int(row[number_name])
        for column, value in expected_row(n).items():
            assert row.get(naming.normalize_path(column)) == value, (n, column)
