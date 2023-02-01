# This is a helper module that contains function which validate data

from typing import Union
from dlt.common.typing import DictStrAny
from re import match
from dlt.common import pendulum

# this string comes before the id
URL_ID_IDENTIFIER = "d"
# time info
SECONDS_PER_DAY = 86400
# TIMEZONE info
DLT_TIMEZONE = "UTC"
# number of seconds from UNIX timestamp origin (1st Jan 1970) to serial number origin (30th Dec 1899)
TIMESTAMP_CONST = -2209161600.0


def is_date_datatype(value_list: list[DictStrAny]) -> list[bool]:
    """
    Helper function that receives a list of value lists from Google Sheets API, and for each data type deduces if the value contains a datetime object or not
    @:param: value_list - a list of the values in the first row of data returned by google sheets api. They are all dicts containing different information about the value
    @:return: value_type_list - list containing bool values. True if the value is a date, False otherwise
    """

    value_type_list = []
    # loop through the list and process each value dict, decide if something is a datetime value or not
    for val_dict in value_list:
        try:
            is_date_type = "DATE" in val_dict["effectiveFormat"]["numberFormat"]["type"]
            is_time_type = "TIME" in val_dict["effectiveFormat"]["numberFormat"]["type"]
            is_date = is_date_type or is_time_type
        except KeyError as e:
            is_date = False
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

    # check if this is an url: http or https in it
    if match(r"http://|https://", url_or_id):
        # process url
        spreadsheet_id = process_url(url_or_id)
        return spreadsheet_id
    else:
        # just return id
        return url_or_id


def serial_date_to_datetime(serial_number: Union[int, float, str, bool]) -> pendulum.datetime:
    """
    This function receives a serial number which can be an int or float(depending on the serial number) and outputs a datetime object
    @:param: serial_number- int/float. The integer part shows the number of days since December 30th 1899, the decimal part shows the fraction of the day. Sometimes if a table is not formatted
    properly this can be also be a bool or str.
    @:return: converted_date: datetime object for the same date as the serial number
    """

    # if called with a different data type, return with whatever input was, handled by the dlt pipeline later - edge case
    if not isinstance(serial_number, (int, float)):
        return serial_number
    # To get the seconds passed since the start date of serial numbers we round the product of the number of seconds in a day and the serial number
    return pendulum.from_timestamp(TIMESTAMP_CONST + round(SECONDS_PER_DAY * serial_number), DLT_TIMEZONE)


def get_first_rows(sheet_range: str) -> list[str]:
    """
    This function receives the range of a Google sheet, parses it and outputs the sheet name, a range which includes the first 2 rows only
    @:param: sheet_range - Ex: sheet1, sheet3!G18:O28. General formula {sheet_name}![Starting_column][Starting_row]:[Ending_column]:[Ending_row]
    @:param: limited_sheet_range - same format but only first 2 rows in range
    @:return: [sheet_name, modified_range] - list containing strings: sheet_name and the range modified to only have the first 2 rows
    """

    # split on the !
    sheet_parts = sheet_range.split("!")
    sheet_name = sheet_parts[0]

    # the range can either have 1 or 2 parts: 1 part if it is simply a sheet name or 2 parts if it is an A1 range
    if len(sheet_parts) == 1:
        return [sheet_name, f"{sheet_name}!1:2"]
    elif len(sheet_parts) > 2:
        raise ValueError("Range format is incorrect! Check documentation for correct usage.")

    range_name = sheet_parts[1]
    # split on the :, expecting strings in the form start:end, i.e 2 parts after splitting on the :
    range_parts = range_name.split(":")
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

    # error handling - parsing errors
    try:
        ending_row = str(int(starting_row) + 1)
    except ValueError:
        raise ValueError(f"Crashed while reading row: {range_start}")
    return [sheet_name, f"{sheet_name}!{starting_col}{starting_row}:{ending_col}{ending_row}"]


def convert_named_range_to_a1(named_range_dict: dict[DictStrAny], sheet_names_dict: dict[DictStrAny] = {}) -> str:
    """
    Converts a named_range dict returned from Google Sheets API metadata call to an A1 range
    @:param: named_range_dict - dict returned from Google Sheets API, it contains all the information about a named range
    @:param: sheet_names_dict - dict containing all the sheets inside the spreadsheet where the sheet id is the key and the sheet name is the corresponding value.
    @:returns: A string which represents the named range as an A1 range.
    """
    start_row_idx = named_range_dict["range"]["startRowIndex"]
    end_row_idx = named_range_dict["range"]["endRowIndex"]
    start_col_idx = named_range_dict["range"]["startColumnIndex"]
    end_col_idx = named_range_dict["range"]["endColumnIndex"]

    # get sheet name from sheet_names_dict
    sheet_id = named_range_dict["range"]["sheetId"]
    named_range_sheet = sheet_names_dict[sheet_id]

    # convert columns from index to letters
    start_col_letter = convert_col_a1(start_col_idx)
    end_col_letter = convert_col_a1(end_col_idx - 1)

    # For some reason the end row index is 1 row beyond the actual stopping point,
    # meaning we don't have to add 1 to convert to row number
    return f"{named_range_sheet}!{start_col_letter}{start_row_idx+1}:{end_col_letter}{end_row_idx}"


def convert_col_a1(col_idx: int) -> str:
    """
    Converts a column index to a column letter in accordance with Google Sheets
    @:param: col_idx - index of column
    @:return: col_name - name of a column
    """
    letters = ["", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"]
    col_name = ""
    while col_idx > 0:
        col_idx, remainder = divmod(col_idx, 26)
        if col_name:
            # edge case - columns of 2 or more letters that start with the letter Z
            if remainder == 0:
                remainder = 26
                col_idx = col_idx - 1
            col_name = letters[remainder] + col_name
        else:
            col_name = letters[remainder+1] + col_name
    return col_name or "A"
