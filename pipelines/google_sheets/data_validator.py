# This is a helper module that contains function which validate data
import re
from typing import Union
from datetime import datetime, timedelta
from re import match
from math import floor


# Date constant- n.o days from 1st Jan 1AD to 30th December 1899
SERIAL_NUMBER_ORIGIN_ORDINAL = 693594
# this string comes before the id
URL_ID_IDENTIFIER = "d"
# time info
SECONDS_IN_DAY = 86400


def process_url(url: str) -> str:
    """"
    This function takes an url to a Google spreadsheet and computes the spreadsheet id from it according to the spreadsheet url formula: https://docs.google.com/spreadsheets/d/<spreadsheet_id>/edit
    If the url is not formatted correctly a Value Error will be returned
    @:param: url- the string containing the url to the spreadsheet
    @:return: spreadsheet_id as a string or ValueError if the url is not properly formatted
    """

    # split on the '/'
    parts = url.split("/")

    # loop through parts
    for i in range(len(parts)):
        # if we find
        if parts[i] == URL_ID_IDENTIFIER and i+1 < len(parts):
            # if the id part is left empty then the url is not formatted correctly
            if parts[i+1] == "":
                raise ValueError("Spreadsheet ID is an empty string")
            else:
                return parts[i+1]
    # if url cannot be found, raise error
    raise ValueError("Invalid URL. Cannot find spreadsheet ID")


def get_spreadsheet_id(url_or_id: str) -> str:
    """"
    This function receives an id or url to a Google Spreadsheet and returns the spreadsheet_id as a string
    @:param: url_or_id a string which is the id or url of the spreadsheet
    @:return: spreadsheet_id a string which is definetly the id of the spreadsheet
    """

    # TODO: raise value error for empty id ?

    # check if this is an url: http or https in it
    if match(r"http://|https://", url_or_id):
        # process url
        spreadsheet_id = process_url(url_or_id)
        return spreadsheet_id
    else:
        # just return id
        return url_or_id


def serial_date_to_datetime(serial_number: Union[int, float]) -> datetime:
    """
    This function receives a serial number which can be an int or float(depending on the serial number) and outputs a datetime object
    @:param: serial_number- int/float. The integer part shows the number of days since December 30th 1899, the decimal part shows the fraction of the day
    @:return: converted_date: datetime object for the same date as the serial number
    """
    # TODO: add timezone to data
    # convert starting date to ordinal date: number of days since Jan 1st, 1 AD
    # the integer part is number of days
    time_since = SERIAL_NUMBER_ORIGIN_ORDINAL + serial_number
    days_since = int(time_since)
    converted_date = datetime.fromordinal(days_since)

    # if this is just a date and not a time data type, just return date simply
    if isinstance(time_since, int):
        return converted_date

    # otherwise get extra seconds passed and add them to datetime
    extra_seconds = round((time_since-days_since) * SECONDS_IN_DAY)
    converted_date = converted_date + timedelta(seconds=extra_seconds)
    return converted_date
