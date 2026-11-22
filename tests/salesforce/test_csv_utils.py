import csv
import io
from typing import List, Optional

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
    # a header without records, a last record without newline, null bytes
    assert list(split_csv_records([HEADER], 10)) == []
    assert list(split_csv_records([], 10)) == []
    pages = list(split_csv_records([HEADER + b'"1","a\x00b"'], 10))
    assert pages == [(HEADER + b'"1","ab"\n', 1)]
