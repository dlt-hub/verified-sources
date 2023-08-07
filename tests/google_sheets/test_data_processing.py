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
