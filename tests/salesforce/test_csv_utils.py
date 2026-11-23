import csv
import io
from typing import Iterator, List, Optional, Tuple

import pytest

from sources.salesforce.helpers.csv_utils import (
    TruncatedCsvError,
    csv_page_to_rows,
    split_csv_records,
)

HEADER = b'"Id","Text"\n'
VALUES = ["a", 'multi\nline, ""quoted""', "", "x" * 50, 'ends with a quote ""\n\n', "z"]
RECORDS = [f'"{n}","{value}"\n'.encode() for n, value in enumerate(VALUES * 5)]
BODY = HEADER + b"".join(RECORDS)


def read_rows(page: bytes) -> List[List[str]]:
    return list(csv.reader(io.StringIO(page.decode())))[1:]


@pytest.mark.parametrize("block_size", [1, 7, 64, 2**20])
@pytest.mark.parametrize(
    "max_records,max_bytes",
    [(7, None), (1000, 120), (4, 120), (1000, None)],
    ids=["max_records", "max_bytes", "both", "one_page"],
)
def test_split_csv_records(
    block_size: int, max_records: int, max_bytes: Optional[int]
) -> None:
    blocks = [BODY[i : i + block_size] for i in range(0, len(BODY), block_size)]
    pages = list(split_csv_records(blocks, max_records, max_bytes))

    assert [row for page, _ in pages for row in read_rows(page)] == read_rows(BODY)
    assert all(page.startswith(HEADER) for page, _ in pages)
    assert [records for _, records in pages] == [
        len(read_rows(page)) for page, _ in pages
    ]
    assert all(records <= max_records for _, records in pages)
    for page, records in pages[:-1]:
        assert records == max_records or (
            max_bytes is not None and len(page) - len(HEADER) >= max_bytes
        )


def test_split_csv_records_edges() -> None:
    # a header without records, null bytes
    assert list(split_csv_records([HEADER], 10)) == []
    assert list(split_csv_records([], 10)) == []
    pages = list(split_csv_records([HEADER + b'"1","a\x00b"\n'], 10))
    assert pages == [(HEADER + b'"1","ab"\n', 1)]


@pytest.mark.parametrize(
    "tail",
    [b'"9","z"', b'"9","multi\nli', b'"9"'],
    ids=["no_newline", "inside_quotes", "missing_field"],
)
def test_split_csv_records_truncated(tail: bytes) -> None:
    # a body that ends inside a record raises, the records of its unfinished page stay unyielded
    pages = split_csv_records([BODY, tail], 7)
    yielded: List[Tuple[bytes, int]] = []
    with pytest.raises(TruncatedCsvError):
        for page in pages:
            yielded.append(page)
    assert sum(records for _, records in yielded) == len(RECORDS) // 7 * 7


def test_split_csv_records_broken_blocks() -> None:
    # an error of the blocks is raised, the records of the unfinished page stay unyielded
    def blocks() -> Iterator[bytes]:
        yield BODY[: len(BODY) // 2]
        raise ConnectionError("broken")

    yielded: List[Tuple[bytes, int]] = []
    with pytest.raises(ConnectionError):
        for page in split_csv_records(blocks(), 4):
            yielded.append(page)
    assert all(records == 4 for _, records in yielded)
    assert [row for page, _ in yielded for row in read_rows(page)] == read_rows(BODY)[
        : 4 * len(yielded)
    ]


def test_csv_page_to_rows_missing_fields() -> None:
    columns = {"Id": {"name": "Id", "data_type": "text"}}
    assert csv_page_to_rows(HEADER + b'"1","a"\n', columns) == [
        {"Id": "1", "Text": "a"}
    ]
    with pytest.raises(ValueError, match="1 of the 2 fields"):
        csv_page_to_rows(HEADER + b'"1"\n', columns)
