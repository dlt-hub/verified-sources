from typing import Any, List, Optional

import pytest


def _insert_message(values: List[Any]) -> bytes:
    message = b"I" + (16384).to_bytes(4, "big") + b"N" + len(values).to_bytes(2, "big")
    for value in values:
        if value is None:
            message += b"n"
        else:
            encoded = value.encode("utf-8")
            message += b"t" + len(encoded).to_bytes(4, "big") + encoded
    return message


@pytest.mark.parametrize("chunk_size", [None, 70], ids=["one-chunk", "three-chunks"])
def test_insert_columns_match_python_parser(
    chunk_size: Optional[int], monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyarrow")
    from sources.pg_replication import arrow_helpers
    from sources.pg_replication.arrow_helpers import insert_columns
    from sources.pg_replication.decoders import read_tuple

    if chunk_size:
        monkeypatch.setattr(arrow_helpers, "INSERT_CHUNK_SIZE", chunk_size)

    rows = [
        ["1", "short", None],
        [None, "", "ü 🦆 ünïcödé"],
        ["3", "x" * 1000, ""],
        ["4", None, None],
    ]
    payloads = [_insert_message(row) for row in rows]
    assert [read_tuple(payload, 6)[0] for payload in payloads] == rows
    if chunk_size:
        # messages of 25 and 38 bytes share a chunk, the 1024 byte message is alone
        chunks = list(arrow_helpers._split_payloads(payloads))
        assert [len(chunk) for chunk in chunks] == [2, 1, 1]
    columns = insert_columns(payloads, 3, [0, 1, 2])
    assert [
        list(values) for values in zip(*(columns[i].to_pylist() for i in range(3)))
    ] == rows
    # excluded columns are skipped
    subset = insert_columns(payloads, 3, [2])
    assert list(subset) == [2] and subset[2].equals(columns[2])
