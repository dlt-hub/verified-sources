"""Files and the performance list seeded into the test site and the values loaded from them"""

from datetime import date, datetime, timedelta, timezone  # noqa: I251
from typing import Any, Dict, List, Tuple

from tests.seed_utils import PERF_STARTS, perf_values
from tests.sharepoint.utils import excel_bytes

TEST_FOLDER = "dlt test"
PAGING_FOLDER = "dlt_paging"
INCREMENTAL_FOLDER = "dlt_incremental"
# more files than one Graph page of 200 children
PAGING_FILES = 205
INCREMENTAL_FILES = 25
REPORT_HEADER = b"order_id,customer,amount,order_date\n"
# a document library besides the default one, its file is not in the default library
ARCHIVE_LIBRARY = "dlt archive"
ARCHIVE_FILE = "archive/report_2023.csv"
ARCHIVE_CONTENT = REPORT_HEADER + b"1,Archived,10.5,2023-12-31\n"

DOCUMENTS: Dict[str, bytes] = {
    f"{TEST_FOLDER}/report_2024_01.csv": REPORT_HEADER
    + b"1,Alice,10.50,2024-01-03\n2,Bob,7.25,2024-01-05\n3,Carla,12.00,2024-01-09\n",
    f"{TEST_FOLDER}/report_2024_02.csv": REPORT_HEADER
    + b"4,Dan,4.00,2024-02-01\n5,Eve,,2024-02-11\n",
    f"{TEST_FOLDER}/report_2024_03.csv": REPORT_HEADER + b"7,Gus,1.00,2024-03-01\n",
    f"{TEST_FOLDER}/report_notes.txt": b"not a csv\n",
    f"{TEST_FOLDER}/summary.csv": b"a,b\n1,2\n",
    f"{TEST_FOLDER}/archive/report_2023_12.csv": REPORT_HEADER
    + b"0,Old,1.00,2023-12-01\n",
    f"{TEST_FOLDER}/R&D (2024)/report_mixed.xlsx": excel_bytes(
        {
            "Data": [
                ["id", "code", "due"],
                [1, 101, date(2024, 3, 1)],
                [2, "A-7", None],
                [3, 103.5, date(2024, 3, 3)],
            ],
            "Notes": [["note"], ["any text"]],
        }
    ),
    "root_sample.csv": b"a,b\n1,2\n3,4\n",
}


def paging_files() -> Dict[str, bytes]:
    return {
        f"{PAGING_FOLDER}/page_{n:03}.csv": f"n\n{n}\n".encode()
        for n in range(1, PAGING_FILES + 1)
    }


def incremental_files() -> List[Tuple[str, bytes, datetime]]:
    """Files of `INCREMENTAL_FOLDER` with unique modification times in 2021, in an order unrelated to their names"""
    start = datetime(2021, 1, 1, tzinfo=timezone.utc)
    # the prime 10000019 wraps the seconds of 2021 every few files. `random` may change between
    # Python versions
    return [
        (
            f"inc_{n:02}.csv",
            f"n\n{n}\n".encode(),
            start + timedelta(seconds=n * 10_000_019 % (365 * 24 * 3600)),
        )
        for n in range(1, INCREMENTAL_FILES + 1)
    ]


# performance list written by `seed.py --performance` with the values of the performance data
# sets of the other sources
PERF_LIST = "dlt perf"
PERF_ITEMS = 20_000
# Graph column definitions, the internal names are the display names
PERF_COLUMNS: List[Dict[str, Any]] = [
    {"name": "Number", "number": {"decimalPlaces": "none"}, "indexed": True},
    {"name": "ChangedAt", "dateTime": {"format": "dateTime"}},
    {"name": "Description", "text": {"allowMultipleLines": True, "textType": "plain"}},
    {"name": "Code", "text": {"maxLength": 16}},
    {"name": "Count", "number": {"decimalPlaces": "none"}},
    {"name": "Amount", "number": {"decimalPlaces": "two"}},
    {"name": "Ratio", "number": {"decimalPlaces": "automatic"}},
    {"name": "Active", "boolean": {}},
    {"name": "Due", "dateTime": {"format": "dateOnly"}},
    {"name": "StartedAt", "dateTime": {"format": "dateTime"}},
]


def graph_datetime(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def perf_item(n: int) -> Dict[str, Any]:
    """Fields of the item `n` of the performance list as written to Graph, without empty ones"""
    values = perf_values(n, PERF_STARTS[0])
    fields = {
        "Title": f"perf item {n}",
        "Number": n,
        "ChangedAt": graph_datetime(values["cursor"]),
        "Description": values["description"],
        "Code": values["code"],
        "Count": values["count"],
        # a string keeps the decimal exact, SharePoint parses it
        "Amount": str(values["amount"]),
        "Ratio": values["ratio"],
        "Active": values["active"],
        "Due": values["due"].isoformat(),
        "StartedAt": graph_datetime(values["started_at"]),
    }
    return {name: value for name, value in fields.items() if value is not None}
