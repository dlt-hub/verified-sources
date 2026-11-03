"""Files seeded into the Documents library of the test site and the values loaded from them"""

import random
from datetime import date, datetime, timedelta, timezone  # noqa: I251
from typing import Dict, List, Tuple

from tests.sharepoint.utils import excel_bytes

TEST_FOLDER = "dlt test"
PAGING_FOLDER = "dlt_paging"
INCREMENTAL_FOLDER = "dlt_incremental"
# more files than one Graph page of 200 children
PAGING_FILES = 205
INCREMENTAL_FILES = 25
REPORT_HEADER = b"order_id,customer,amount,order_date\n"

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
    seconds = random.Random(2021).sample(range(365 * 24 * 3600), INCREMENTAL_FILES)
    start = datetime(2021, 1, 1, tzinfo=timezone.utc)
    return [
        (f"inc_{n:02}.csv", f"n\n{n}\n".encode(), start + timedelta(seconds=s))
        for n, s in enumerate(seconds, 1)
    ]
