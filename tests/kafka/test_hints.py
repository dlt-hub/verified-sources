from typing import Any, Dict

import pytest

from sources.kafka.helpers.hints import column_type_fits


@pytest.mark.parametrize(
    "existing,new,fits",
    [
        ({"data_type": "bigint"}, {"data_type": "bigint"}, True),
        ({"data_type": "bigint"}, {"data_type": "double"}, False),
        ({"data_type": "text"}, {"data_type": "binary"}, False),
        (
            {"data_type": "decimal", "precision": 12, "scale": 2},
            {"data_type": "decimal", "precision": 10, "scale": 2},
            True,
        ),
        (
            {"data_type": "decimal", "precision": 10, "scale": 2},
            {"data_type": "decimal", "precision": 12, "scale": 2},
            False,
        ),
        (
            {"data_type": "decimal", "precision": 10, "scale": 2},
            {"data_type": "decimal", "precision": 10, "scale": 3},
            False,
        ),
        # a decimal without precision has the default precision of dlt
        (
            {"data_type": "decimal"},
            {"data_type": "decimal", "precision": 20, "scale": 9},
            True,
        ),
        (
            {"data_type": "timestamp", "timezone": True},
            {"data_type": "timestamp", "timezone": False},
            False,
        ),
        ({"data_type": "text", "precision": 50}, {"data_type": "text"}, False),
        ({"data_type": "text"}, {"data_type": "text", "precision": 50}, True),
    ],
    ids=[
        "same",
        "bigint_to_double",
        "text_to_binary",
        "narrower_decimal",
        "wider_decimal",
        "more_decimal_places",
        "default_decimal",
        "timestamp_time_zone",
        "longer_text",
        "bounded_text",
    ],
)
def test_column_type_fits(
    existing: Dict[str, Any], new: Dict[str, Any], fits: bool
) -> None:
    assert column_type_fits(existing, new) is fits  # type: ignore[arg-type]
