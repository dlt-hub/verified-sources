# This is a helper module that contains function which validate data

from typing import Union
from dateutil import parser
from datetime import datetime

import pytest


# this function takes an url to a Google spreadsheet and computes the spreadsheet id from it. returns an empty string if the url format is not correct
# spreadsheet url formula: https://docs.google.com/spreadsheets/d/<spreadsheet_id>/edit
# @input: url- str
# @output spreadsheet_id: str
def process_url(url: str) -> str:

    # this string comes before the id
    url_id_identifier = "d"
    # split on the '/'
    parts = url.split("/")

    # loop through parts
    for i in range(len(parts)):
        # if we find
        if parts[i] == url_id_identifier and i+1 < len(parts):
            if parts[i+1] == "":
                raise ValueError("Spreadsheet ID is an empty string")
            return parts[i+1]
    # if url cannot be found, raise error
    raise ValueError("Invalid URL. Cannot find spreadsheet ID")


# This function receives a serial number which can be an int or float(depending on the serial number) and outputs a datetime object
# @input: serial_number- int/float. The integer part shows the number of days since December 30th 1899, the decimal part shows the fraction of the day
# @output: converted_date: datetime object for the same date as the serial number
def serial_date_to_datetime(serial_number: Union[int, float]) -> datetime:

    # convert starting date to ordinal date: number of days since Jan 1st, 1 AD
    days_since = datetime(1899, 12, 30).toordinal() + serial_number

    # convert back to datetime
    converted_date = datetime.fromordinal(days_since)
    return converted_date


########## TEMP TESTS ############
TEST_CASES_URL = [
    ['https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit#gid=0', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'],
    ['https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit?usp=sharing', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'],
    ['https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'],
    ['https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNopQrStUvWxYz1234567890/edit#gid=0&new=true', '1aBcDeFgHiJkLmNopQrStUvWxYz1234567890'],
    ['https://docs.google.com/spreadsheets/', ValueError("Invalid URL or cannot find spreadsheet ID")],
    ['https://docs.google.com/spreadsheets/d', ValueError("Invalid URL or cannot find spreadsheet ID")],
    ['https://docs.google.com/spreadsheets/d/', ValueError("Invalid URL or cannot find spreadsheet ID")]
]


# TODO: implement some proper testing for date conversion
TEST_CASES_DATE = [
    '12/31/2022',
    '31-12-2022',
    '23:59:59',
    '11:59 PM',
    '12/31/2022 23:59:59',
    '12-31-2022 23:59:59'
]


# tester for url conversion
@pytest.mark.parametrize("url", "expected", TEST_CASES_URL)
def process_url_tester(url: str, expected: str):

    try:
        assert process_url(url) == expected
    except ValueError as e:
        assert str(e) == str(expected)





