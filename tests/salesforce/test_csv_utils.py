import csv
import io
from typing import List

import pytest

from sources.salesforce.helpers.csv_utils import split_csv_records

HEADER = b'"Id","Text"\n'
VALUES = ["a", 'multi\nline, ""quoted""', "", "x" * 50, 'ends with a quote ""\n\n', "z"]
RECORDS = [f'"{n}","{value}"\n'.encode() for n, value in enumerate(VALUES * 5)]
BODY = HEADER + b"".join(RECORDS)


def read_rows(page: bytes) -> List[List[str]]:
    return list(csv.reader(io.StringIO(page.decode())))[1:]


@pytest.mark.parametrize("block_size", [1, 7, 64, 2**20])
@pytest.mark.parametrize(
    "max_records,max_bytes,skip",
    [(7, 2**20, 0), (1000, 120, 0), (4, 2**20, 13), (1000, 2**20, 30)],
    ids=["max_records", "max_bytes", "skip", "skip_all"],
)
def test_split_csv_records(
    block_size: int, max_records: int, max_bytes: int, skip: int
) -> None:
    blocks = [BODY[i : i + block_size] for i in range(0, len(BODY), block_size)]
    pages = list(split_csv_records(blocks, max_records, max_bytes, skip))

    expected = read_rows(BODY)[skip:]
    assert [row for page, _ in pages for row in read_rows(page)] == expected
    assert all(page.startswith(HEADER) for page, _ in pages)
    assert [records for _, records in pages] == [
        len(read_rows(page)) for page, _ in pages
    ]
    assert all(records <= max_records for _, records in pages)


def test_split_csv_records_edges() -> None:
    # a header without records, a last record without newline, null bytes
    assert list(split_csv_records([HEADER], 10, 2**20)) == []
    assert list(split_csv_records([], 10, 2**20)) == []
    pages = list(split_csv_records([HEADER + b'"1","a\x00b"'], 10, 2**20))
    assert pages == [(HEADER + b'"1","ab"\n', 1)]
