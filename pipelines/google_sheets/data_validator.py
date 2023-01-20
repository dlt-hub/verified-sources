# This is a helper module that contains function which validate data
from typing import Union
from dlt.common.typing import DictStrAny
from datetime import datetime, timedelta
from re import match


# Date constant- n.o days from 1st Jan 1AD to 30th December 1899
SERIAL_NUMBER_ORIGIN_ORDINAL = 693594
# this string comes before the id
URL_ID_IDENTIFIER = "d"
# time info
SECONDS_IN_DAY = 86400


def get_data_type(value_list: list[DictStrAny]) -> list[bool]:
    """
    This is a helper function that receives
    @:param: value_list - a list of the values in the first row of data returned by google sheets api. They are all dicts containing different information about the value
    @:return: value_type_list - list containing bool values. True if the value is a date, False otherwise
    """

    # TODO: check if row and cols are empty and skip

    value_type_list = []
    # loop through the list and process each value dict, decide if something is a datetime value or not
    for val_dict in value_list:
        # TODO: try except
        is_date = "effectiveValue" in val_dict and \
                  "numberValue" in val_dict['effectiveValue'] and \
                  "effectiveFormat" in val_dict and \
                  "numberFormat" in val_dict['effectiveFormat'] and\
                  "type" in val_dict["effectiveFormat"]["numberFormat"] and\
                  ("DATE" in val_dict["effectiveFormat"]["numberFormat"]["type"] or "TIME" in val_dict["effectiveFormat"]["numberFormat"]["type"])
        value_type_list.append(is_date)
    return value_type_list


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

    # if called with a different data type, return with whatever input was, handled by the dlt pipeline later
    if not isinstance(serial_number, (int, float)):
        return serial_number

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


def get_first_rows(sheet_range: str) -> list[str]:
    """
    This function receives the range of a Google sheet, parses it and outputs the sheet name, a range which includes the first 2 rows only
    @:param: sheet_range - Ex: sheet1, sheet3!G18:O28. General formula {sheet_name}![Starting_column][Starting_row]:[Ending_column]:[Ending_row]
    @:param: limited_sheet_range - same format but only first 2 rows in range
    @:return: [sheet_name, modified_range] - list containing strings: sheet_name and the range modified to only have the first 2 rows
    """

    # TODO : add parsing for R1C1 notation

    # split on the !
    sheet_parts = sheet_range.split("!")
    sheet_name = sheet_parts[0]

    # this is just a sheet name if it only has 1 part
    if len(sheet_parts) == 1:
        return [sheet_name, f"{sheet_name}!1:2"]
    # if for some reason there are not 2 parts, raise value error with range
    elif len(sheet_parts) != 2:
        raise ValueError("Range format is incorrect! Check documentation for correct usage.")

    range_name = sheet_parts[1]

    # split on the :
    range_parts = range_name.split(":")

    # again check for misformated ranges
    if len(range_parts) != 2:
        raise ValueError("Range format is incorrect! Check documentation for correct usage.")

    range_start = range_parts[0]
    range_end = range_parts[1]

    # iterate through the range start to specify starting row and starting column
    starting_row = ""
    starting_col = ""
    i = 0
    while i < len(range_start) and not range_start[i].isdigit():
        # register starting column
        starting_col = starting_col + range_start[i]
        # update
        i = i + 1

    # register starting row
    while i < len(range_start) and range_start[i].isdigit():
        starting_row = starting_row + range_start[i]
        i = i + 1

    # iterate through the range start to specify starting row and starting column
    ending_col = ""
    i = 0
    while i < len(range_end) and not range_end[i].isdigit():
        # register starting column
        ending_col = ending_col + range_end[i]
        # update
        i = i + 1

    # handle start_col:end_col format
    if not starting_row:
        starting_row = "1"

    # error handling incase of parsing errors
    try:
        ending_row = str(int(starting_row) + 1)
    except ValueError:
        raise ValueError(f"Crashed while reading row: {range_start}")

    return [sheet_name, f"{sheet_name}!{starting_col}{starting_row}:{ending_col}{ending_row}"]
