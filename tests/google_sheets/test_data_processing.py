import pytest
from sources.google_sheets.helpers import data_processing
from typing import Union, List

from dlt.common.typing import DictStrAny
from dlt.common import pendulum

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


def test_get_data_types_from_multiple_rows():
    """Test combining metadata from multiple rows to determine data types."""
    row1_metadata = [
        {"effectiveValue": {"stringValue": "a"}, "formattedValue": "a"}
    ]
    
    row2_metadata = [
        {"effectiveValue": {"stringValue": "b"}, "formattedValue": "b"},
        {"effectiveValue": {"stringValue": "c"}, "formattedValue": "c"}
    ]
    
    data_types = data_processing.get_data_types_from_multiple_rows([row1_metadata, row2_metadata], 2)
    
    assert len(data_types) == 2
    assert data_types == [None, None]


def test_get_data_types_from_multiple_rows_with_datetime():
    """Test datetime detection across multiple rows."""
    row1_metadata = [
        {"effectiveValue": {"stringValue": "a"}, "formattedValue": "a"}
    ]
    
    row2_metadata = [
        {"effectiveValue": {"stringValue": "b"}, "formattedValue": "b"},
        {
            "effectiveValue": {"numberValue": 44927.0},
            "formattedValue": "1/1/2023",
            "effectiveFormat": {"numberFormat": {"type": "DATE_TIME"}}
        }
    ]
    
    data_types = data_processing.get_data_types_from_multiple_rows([row1_metadata, row2_metadata], 2)
    
    assert len(data_types) == 2
    assert data_types == [None, "timestamp"]


def test_get_data_types_from_multiple_rows_edge_cases():
    """Test edge cases: empty input, empty rows, and padding."""
    assert data_processing.get_data_types_from_multiple_rows([], 3) == [None, None, None]
    assert data_processing.get_data_types_from_multiple_rows([[], []], 2) == [None, None]
    
    single_col_data = [{"effectiveValue": {"stringValue": "a"}, "formattedValue": "a"}]
    result = data_processing.get_data_types_from_multiple_rows([single_col_data], 5)
    assert len(result) == 5
    assert result == [None, None, None, None, None]


def test_get_data_types_from_multiple_rows_preserves_columns():
    """Test that all expected columns are preserved even when missing from early rows."""
    headers_count = 2
    
    incomplete_row = [{"effectiveValue": {"stringValue": "a"}, "formattedValue": "a"}]
    complete_row = [
        {"effectiveValue": {"stringValue": "b"}, "formattedValue": "b"},
        {"effectiveValue": {"stringValue": "c"}, "formattedValue": "c"}
    ]
    
    result = data_processing.get_data_types_from_multiple_rows([incomplete_row, complete_row], headers_count)
    
    assert len(result) == 2
    assert result == [None, None]
