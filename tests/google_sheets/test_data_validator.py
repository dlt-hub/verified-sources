import pytest
from pipelines.google_sheets import data_validator
from datetime import datetime
from typing import Union
from dlt.common.typing import TDataItems, DictStrAny
from dlt.common import pendulum


TEST_CASES_URL = [
    ['https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit#gid=0', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'],
    ['https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit?usp=sharing', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'],
    ['https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'],
    ['https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit#gid=0&new=true', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'],
    ['https://docs.google.com/spreadsheets/', ValueError("Invalid URL. Cannot find spreadsheet ID")],
    ['https://docs.google.com/spreadsheets/d', ValueError("Invalid URL. Cannot find spreadsheet ID")],
    ['https://docs.google.com/spreadsheets/d/', ValueError("Spreadsheet ID is an empty string")]
]

TEST_CASES_URL_OR_ID = [
    ('https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit#gid=0', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'),
    ('https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit?usp=sharing', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'),
    ('https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'),
    ('https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit#gid=0&new=true', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'),
    ('https://docs.google.com/spreadsheets/', ValueError("Invalid URL. Cannot find spreadsheet ID")),
    ('https://docs.google.com/spreadsheets/d', ValueError("Invalid URL. Cannot find spreadsheet ID")),
    ('https://docs.google.com/spreadsheets/d/', ValueError("Spreadsheet ID is an empty string")),
    ('1aBcDeFgHiJkLmNopQrStUvWxYz1234567890', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'),
    ('', '')
]

# TODO: implement some proper testing for date conversion
TEST_CASES_DATE = [
    (37621, pendulum.datetime(year=2002, month=12, day=31, tz="UTC")),
    (0.999988425925926, pendulum.datetime(year=1899, month=12, day=30, hour=23, minute=59, second=59, tz="UTC")),
    (0.9993055555555556, pendulum.datetime(year=1899, month=12, day=30, hour=23, minute=59, tz="UTC")),
    (44926.99998842592, pendulum.datetime(year=2022, month=12, day=31, hour=23, minute=59, second=59, tz="UTC"))
]

TEST_CASES_RANGE = [
    ("sheet1", ["sheet1", "sheet1!1:2"]),
    ("sheet1!G2:O28", ["sheet1", "sheet1!G2:O3"]),
    ("sheet1!G2:H28", ["sheet1", "sheet1!G2:H3"]),
    ("sheet1!A:B", ["sheet1", "sheet1!A1:B2"]),
    ("sheet1!1:4", ["sheet1", "sheet1!1:2"])
]

TEST_CASES_DATA_TYPES = [
    ([], []),
    ([], []),
    ([], []),
    ([], [])
]

TEST_CASES_CONVERT_COL = [
    (0, "A"),
    (1, "B"),
    (25, "Z"),
    (26, "AA"),
    (27, "AB"),
    (51, "AZ"),
    (52, "BA"),
    (53, "BB"),
    (675, "YZ"),
    (676, "ZA"),
    (700, "ZY"),
    (701, "ZZ"),
    (702, "AAA")
]


@pytest.mark.parametrize("url, expected", TEST_CASES_URL)
def test_process_url(url: str, expected: str):
    """
    Tester for process_url function
    :param: url- url input str
    :param: expected: expected output str
    """

    try:
        assert data_validator.process_url(url) == expected
    except ValueError as e:
        assert str(e) == str(expected)


@pytest.mark.parametrize("url_or_id, expected", TEST_CASES_URL_OR_ID)
def test_get_spreadsheet_id(url_or_id: str, expected: str):
    """
    Tester for get_spreadsheet_id function
    :param: url_or_id- url or id input str
    :param: expected: expected output str
    """

    try:
        assert data_validator.get_spreadsheet_id(url_or_id) == expected
    except ValueError as e:
        assert str(e) == str(expected)


@pytest.mark.parametrize("serial_number, expected", TEST_CASES_DATE)
def test_serial_date_to_datetime(serial_number: Union[int, float], expected: datetime):
    """
    Tester for serial_date_to_datetime function
    :param: serial_number- float or int date input
    :param: expected: expected output datetime
    """

    assert data_validator.serial_date_to_datetime(serial_number) == expected


@pytest.mark.parametrize("sheet_range, expected", TEST_CASES_RANGE)
def test_ranges(sheet_range: str, expected: str):

    assert data_validator.get_first_rows(sheet_range) == expected


@pytest.mark.parametrize("value_dict_row, expected", TEST_CASES_DATA_TYPES)
def test_data_types(value_dict_row: list[DictStrAny], expected: bool):
    assert data_validator.is_date_datatype(value_dict_row) == expected


@pytest.mark.parametrize("col_idx, expected", TEST_CASES_CONVERT_COL)
def test_data_types(col_idx: int, expected: str):
    assert data_validator.convert_col_a1(col_idx) == expected
