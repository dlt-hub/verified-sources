"""This is a helper module that contains function which validate and process data"""


from typing import Any, Dict, Iterator, List, Tuple, Union
from re import match
from dlt.common import logger, pendulum
from dlt.common.typing import DictStrAny

# this string comes before the id
URL_ID_IDENTIFIER = "d"
# time info
SECONDS_PER_DAY = 86400
# TIMEZONE info
DLT_TIMEZONE = "UTC"
# number of seconds from UNIX timestamp origin (1st Jan 1970) to serial number origin (30th Dec 1899)
TIMESTAMP_CONST = -2209161600.0


def get_spreadsheet_id(url_or_id: str) -> str:
    """"
    Receives an id or url to a Google Spreadsheet and returns the spreadsheet_id as a string
    @:param: url_or_id a string which is the id or url of the spreadsheet
    @:return: spreadsheet_id a string which is definitely the id of the spreadsheet
    """

    # check if this is an url: http or https in it
    if match(r"http://|https://", url_or_id):
        # process url
        spreadsheet_id = process_url(url_or_id)
        return spreadsheet_id
    else:
        # just return id
        return url_or_id


def process_url(url: str) -> str:
    """"
    Takes an url to a Google spreadsheet and computes the spreadsheet id from it according to the spreadsheet url formula: https://docs.google.com/spreadsheets/d/<spreadsheet_id>/edit
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


def get_first_rows(sheet_range: str) -> List[str]:
    """
    Receives the range of a Google sheet, parses it and outputs the sheet name, a range which includes the first 2 rows only. Is used for only getting the first 2 rows when collecting metadata.
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
    # separate row and column letters from both the range start and end
    range_parts = range_name.split(":")
    if len(range_parts) != 2:
        raise ValueError("Range format is incorrect! Check documentation for correct usage.")
    starting_row, starting_col = _separate_row_col(range_parts[0])
    ending_row, ending_col = _separate_row_col(range_parts[1])

    # handle possible edge cases/errors
    # start_col:end_col format
    if not starting_row:
        starting_row = "1"
    # handle parsing errors and determine new end row
    try:
        ending_row = str(int(starting_row) + 1)
    except ValueError:
        raise ValueError(f"Crashed while reading range part: {range_parts[0]}")
    return [sheet_name, f"{sheet_name}!{starting_col}{starting_row}:{ending_col}{ending_row}"]


def _separate_row_col(row_col_str: str) -> Tuple[str, str]:
    """
    Helper function that receives a row and column together from the A1 range and returns the row and column separately
    @:param: row_col_str - ex: A1, BB2, ZZ25, etc
    @:return range_row, range_col - ex: ("A", "1"), etc
    """
    range_row = ""
    range_col = ""
    for range_char in row_col_str:
        if range_char.isdigit():
            range_row += range_char
        else:
            range_col += range_char
    return range_row, range_col


def convert_named_range_to_a1(named_range_dict: DictStrAny, sheet_names_dict: Dict[str, DictStrAny] = None) -> str:
    """
    Converts a named_range dict returned from Google Sheets API metadata call to an A1 range
    @:param: named_range_dict - dict returned from Google Sheets API, it contains all the information about a named range
    @:param: sheet_names_dict - dict containing all the sheets inside the spreadsheet where the sheet id is the key and the sheet name is the corresponding value.
    @:returns: A string which represents the named range as an A1 range.
    """

    if not sheet_names_dict:
        sheet_names_dict = {}
    start_row_idx = named_range_dict["range"]["startRowIndex"]
    end_row_idx = named_range_dict["range"]["endRowIndex"]
    start_col_idx = named_range_dict["range"]["startColumnIndex"]
    end_col_idx = named_range_dict["range"]["endColumnIndex"]

    # get sheet name from sheet_names_dict
    sheet_id = named_range_dict["range"]["sheetId"]
    named_range_sheet = sheet_names_dict[sheet_id]

    # convert columns from index to letters
    start_col_letter = _convert_col_a1(start_col_idx)
    end_col_letter = _convert_col_a1(end_col_idx - 1)

    # For some reason the end row index is 1 row beyond the actual stopping point,
    # meaning we don't have to add 1 to convert to row number
    return f"{named_range_sheet}!{start_col_letter}{start_row_idx+1}:{end_col_letter}{end_row_idx}"


def _convert_col_a1(col_idx: int) -> str:
    """
    Helper, converts a column index to a column letter in accordance with Google Sheets
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


def get_range_headers(range_metadata: List[DictStrAny], range_name: str) -> List[str]:
    """
    Helper. Receives the metadata for a range of cells and outputs only the headers for columns, i.e. first line of data
    @:param: range_metadata - Dict containing metadata for the first 2 rows of a range, taken from Google Sheets API response.
    @:param: range_name - the name of the range as appears in the metadata, might be different from actual range name if this is a named range. Needed for error reporting
    @:return: headers - list of headers
    """
    headers = []
    empty_header_index = 0
    for header in range_metadata[0]["values"]:
        if header:
            header_val = header["formattedValue"]
            # warn user when reading non string values as header - metadata is the only place to check this info
            if not ("stringValue" in header["effectiveValue"]):
                logger.warning(f"In range {range_name}, header value: {header_val} is not a string! Name changed when loaded in database!")
            headers.append(header_val)
        else:
            headers.append(f"empty_header_filler{empty_header_index}")
            empty_header_index = empty_header_index + 1
    # report if a header was empty
    if empty_header_index:
        logger.warning(f"In range {range_name}, {empty_header_index} headers were found empty!")
    # manage headers being empty
    if len(headers) == empty_header_index:
        return []
    return headers


def get_first_line(range_metadata: List[DictStrAny]) -> List[bool]:
    """
    Helper. Parses through the metadata for a range and checks whether a column contains datetime types or not
    @:param: range_metadata - Metadata for first 2 rows in a range
    @:return: is_datetime_cols - list containing True of False depending on whether the first line of data is a datetime object or not.
    """

    # get data for 1st column and process them, if empty just return an empty list
    try:
        is_datetime_cols = is_date_datatype(range_metadata[1]["values"])
    except IndexError:
        return []
    return is_datetime_cols


def is_date_datatype(value_list: List[DictStrAny]) -> List[bool]:
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
        except KeyError:
            is_date = False
        value_type_list.append(is_date)
    return value_type_list


def serial_date_to_datetime(serial_number: Union[int, float, str, bool]) -> Union[pendulum.DateTime, str, bool]:
    """
    Receives a serial number which can be an int or float(depending on the serial number) and outputs a datetime object
    @:param: serial_number- int/float. The integer part shows the number of days since December 30th 1899, the decimal part shows the fraction of the day. Sometimes if a table is not formatted
    properly this can be also be a bool or str.
    @:return: converted_date: datetime object for the same date as the serial number
    """

    # if called with a different data type, return with whatever input was, handled by the dlt pipeline later - edge case
    if not isinstance(serial_number, (int, float)):
        return serial_number
    # To get the seconds passed since the start date of serial numbers we round the product of the number of seconds in a day and the serial number
    conv_datetime: pendulum.DateTime = pendulum.from_timestamp(0, DLT_TIMEZONE) + pendulum.duration(seconds=TIMESTAMP_CONST + round(SECONDS_PER_DAY * serial_number))
    return conv_datetime


def metadata_preprocessing(ranges: List[str], named_ranges: DictStrAny = None) -> Tuple[List[str], Dict[str, List[DictStrAny]]]:
    """
    Helper, will iterate through input ranges and process them so only the first 2 rows are returned per range. It will also structure all the ranges inside a dict similar to how they are returned
    by the Google Sheets API metadata request
    @:param: ranges - list of range names
    @:param named_ranges: dict containing ranges as keys and the corresponding named ranges as the values
    @:return meta_ranges: List containing all the ranges where metadata is gathered from, ex: sheet1!1:2 would be an element of this list
    @:return response_like_dict: This dictionary copies how google sheets API returns data from ranges: all ranges are organized into their parent sheets which are keys to the dict.
                                 All ranges belonging to a sheet are in a list with the order in which they appear in the request being preserved, ex: {"sheet1" : ["range3", "range1"]
                                 range3 is returned before range1 because it would have been ahead in the list of ranges given in the request. Here instead of storing a string for
                                 range1 or range3, a dict with all the metadata for that range is stored. as showed in unfilled_range_dict
    """

    # process metadata ranges so only the first 2 rows are appended
    # response like dict will contain a dict similar to the response by the Google Sheets API: ranges are returned inside the sheets they belong in the order given in the API request.
    meta_ranges = []
    response_like_dict: Dict[str, List[DictStrAny]] = {}
    for requested_range in ranges:
        # Metadata ranges have a different range-only first 2 rows, so we need to convert to those ranges first
        # convert range to first 2 rows, getting the sheet name and the range that has only the first 2 rows
        range_info = get_first_rows(requested_range)
        sheet_name = range_info[0]
        # initialize the dict containing information about the metadata of this range, headers contains the names of all header columns and
        # cols_is_date contains booleans indicating whether the data expected in that column is a datetime object or not.
        unfilled_range_dict = {"range": requested_range,
                               "headers": [],
                               "cols_is_datetime": []
                               }
        # try to fill information about range name if the range has a name by checking named ranges
        try:
            unfilled_range_dict["name"] = named_ranges[requested_range]
        except (KeyError, TypeError):
            unfilled_range_dict["name"] = None
        # All the information in the dict is properly set up, now we just need to store it in the response_like_dict
        try:
            response_like_dict[sheet_name].append(unfilled_range_dict)
        except KeyError:
            response_like_dict[sheet_name] = [unfilled_range_dict]
        meta_ranges.append(range_info[1])
    return meta_ranges, response_like_dict


def process_range(sheet_val: List[List[Any]], sheet_meta: DictStrAny) -> Iterator[DictStrAny]:
    """
    Receives 2 arrays of tabular data inside a sheet. This will be processed into a schema that is later stored into a database table
    @:param: sheet_val - 2D array of values
    @:param: sheet_meta - Metadata gathered for this specific range
    @:return:  table_dict - a dict version of the table. It generates a dict of the type {header:value} for every row.
    """
    # get headers and first line data types which is just Datetime or not Datetime so far and loop through the remaining values
    headers = sheet_meta["headers"]
    first_line_val_types = sheet_meta["cols_is_datetime"]
    # edge case - only 1 line of data, load the empty tables
    if len(sheet_val) == 1:
        yield {header: "" for header in headers}
    # otherwise loop through the other rows and return data normally
    for row in sheet_val[1:]:
        table_dict = {}
        # empty row; skip
        if not row:
            continue
        # process both rows and check for differences to spot dates
        for val, header, is_datetime in zip(row, headers, first_line_val_types):
            # 3 main cases: null cell value, datetime value, every other value
            # handle null values properly. Null cell values are returned as empty strings, this will cause dlt to create new columns and fill them with empty strings
            if val == "":
                fill_val = None
            elif is_datetime:
                fill_val = serial_date_to_datetime(val)
            else:
                fill_val = val
            table_dict[header] = fill_val
        yield table_dict
