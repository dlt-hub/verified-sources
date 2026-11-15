from datetime import datetime, timezone
from typing import Any, Dict

import pytest
from confluent_kafka import TIMESTAMP_CREATE_TIME, TIMESTAMP_NOT_AVAILABLE, Message
from dlt.common.utils import digest128

from sources.kafka.helpers.processors import default_msg_processor

from tests.kafka.utils import message

MS = 1731412345678


@pytest.mark.parametrize(
    "msg,expected",
    [
        (message("t", 0, 1), {"key": "k", "data": "v"}),
        (message("t", 0, 1, key=None), {"key": None, "data": "v"}),
        (message("t", 0, 1, value=None), {"key": "k", "data": None}),
        (
            message("t", 0, 1, value=b"\x00\x00\x00\x00\x07\xff"),
            {"data": b"\x00\x00\x00\x00\x07\xff"},
        ),
        (message("t", 0, 1, key=b"\x00\xff"), {"key": b"\x00\xff"}),
        (
            message("t", 0, 1, timestamp=(TIMESTAMP_CREATE_TIME, MS)),
            {
                "ts": {
                    "type": 1,
                    "value": datetime(2024, 11, 12, 11, 52, 25, 678000, timezone.utc),
                }
            },
        ),
        (
            message("t", 0, 1, timestamp=(TIMESTAMP_NOT_AVAILABLE, -1)),
            {"ts": {"type": 0, "value": None}},
        ),
    ],
    ids=[
        "text",
        "null_key",
        "tombstone",
        "binary_value",
        "binary_key",
        "timestamp_ms",
        "no_timestamp",
    ],
)
def test_default_msg_processor(msg: Message, expected: Dict[str, Any]) -> None:
    row = default_msg_processor(msg)
    assert row["_kafka"]["topic"] == "t"
    assert row["_kafka"]["partition"] == 0
    assert row["_kafka"]["offset"] == 1
    for name, value in expected.items():
        assert row["_kafka"][name] == value
    # unchanged from earlier versions, existing tables keep their values
    assert row["_kafka_msg_id"] == digest128("t0" + str(row["_kafka"]["key"]))
