import pytest
from sources.google_sheets.helpers import data_processing
from typing import Union, List, Any

from dlt.common.typing import DictStrAny
from dlt.common import pendulum
from dlt.common.data_types import TDataType

TEST_CASES_URL = [
    (
        "https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit#gid=0",
        "1aBcDeFgHiJkLmNopQrStUvWxYz1234567890",
    ),
    (
        "https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit?usp=sharing",
        "1aBcDeFgHiJkLmNopQrStUvWxYz1234567890",
    ),
    (
        "https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890",
        "1aBcDeFgHiJkLmNopQrStUvWxYz1234567890",
    ),
    (
        "https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit#gid=0&new=true",
        "1aBcDeFgHiJkLmNopQrStUvWxYz1234567890",
    ),
    (
        "https://docs.google.com/spreadsheets/",
        None,
    ),
    (
        "https://docs.google.com/spreadsheets/d",
        None,
    ),
    (
        "https://docs.google.com/spreadsheets/d/",
        None,
    ),
]
TEST_CASES_URL_OR_ID = [
    (
        "https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit#gid=0",
        "1aBcDeFgHiJkLmNopQrStUvWxYz1234567890",
    ),
    (
        "https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit?usp=sharing",
        "1aBcDeFgHiJkLmNopQrStUvWxYz1234567890",
    ),
    (
        "https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890",
        "1aBcDeFgHiJkLmNopQrStUvWxYz1234567890",
    ),
    (
        "https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit#gid=0&new=true",
        "1aBcDeFgHiJkLmNopQrStUvWxYz1234567890",
    ),
    (
        "https://docs.google.com/spreadsheets/",
        None,
    ),
    (
        "https://docs.google.com/spreadsheets/d",
        None,
    ),
    (
        "https://docs.google.com/spreadsheets/d/",
        None,
    ),
    ("1aBcDeFgHiJkLmNopQrStUvWxYz1234567890", "1aBcDeFgHiJkLmNopQrStUvWxYz1234567890"),
    ("", ""),
]

TEST_CASES_DATE = [
    (37621, pendulum.datetime(year=2002, month=12, day=31, tz="UTC")),
    (
        0.999988425925926,
        pendulum.datetime(
            year=1899, month=12, day=30, hour=23, minute=59, second=59, tz="UTC"
        ),
    ),
    (
        0.9993055555555556,
        pendulum.datetime(year=1899, month=12, day=30, hour=23, minute=59, tz="UTC"),
    ),
    (
        44926.99998842592,
        pendulum.datetime(
            year=2022, month=12, day=31, hour=23, minute=59, second=59, tz="UTC"
        ),
    ),
]


@pytest.mark.parametrize("url, expected", TEST_CASES_URL)
def test_process_url(url: str, expected: str):
    """
    Tester for process_url function
    :param: url- url input str
    :param: expected: expected output str
    """
    try:
        assert data_processing.extract_spreadsheet_id_from_url(url) == expected
    except ValueError:
        assert expected is None


@pytest.mark.parametrize("url_or_id, expected", TEST_CASES_URL_OR_ID)
def test_get_spreadsheet_id(url_or_id: str, expected: str):
    """
    Tester for get_spreadsheet_id function
    :param: url_or_id- url or id input str
    :param: expected: expected output str
    """
    try:
        assert data_processing.get_spreadsheet_id(url_or_id) == expected
    except ValueError:
        assert expected is None


@pytest.mark.parametrize("serial_number, expected", TEST_CASES_DATE)
def test_serial_date_to_datetime(
    serial_number: Union[int, float], expected: pendulum.DateTime
):
    """
    Tester for serial_date_to_datetime function
    :param: serial_number- float or int date input
    :param: expected: expected output datetime
    """
    assert (
        data_processing.serial_date_to_datetime(serial_number, "timestamp") == expected
    )


@pytest.mark.parametrize(
    "sheet_values",
    [
        [
            [1],
            [2, "a", "b", "c", "d", "f"],
        ],
        [
            [322, "", "", 2, "", 123456],
            [43, "dsa", "dd", "w", 2],
            [432, "scds", "ddd", "e", 3],
            ["", "dsfdf", "dddd", "r", 4],
        ],
        [
            [322, "", "", 2],
            [43, "dsa", "dd", "w", 2],
            [432, "scds", "ddd", "e", 3],
            ["", "dsfdf", "dddd", "r", 4],
        ],
    ],
)
def test_process_range_with_missing_cols(
    sheet_values: List[List[Any]],
):
    """
    Tester for process_range function
    :param: sheet_values- list of
    """
    # Define 6 headers
    headers = [f"col{i}" for i in range(1, 7)]

    # Create dummy data types aligned with the number of columns in the first row
    data_types = [None for i in range(len(sheet_values[0]))]

    table_dicts = list(data_processing.process_range(sheet_values, headers, data_types))

    for i, table_dict in enumerate(table_dicts):
        # Mid row empty values are handled as None
        expected_dict_vals = [val if val != "" else None for val in sheet_values[i]]
        # Trailing empty columns that are not detected should be added as None
        expected_dict_vals += [None] * (len(headers) - len(sheet_values[i]))
        expected_dict = {f"col{i+1}": val for i, val in enumerate(expected_dict_vals)}
        assert expected_dict == table_dict
        assert len(table_dict) == 6
