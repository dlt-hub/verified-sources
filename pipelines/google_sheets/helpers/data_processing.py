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
    """
    Receives an ID or URL to a Google Spreadsheet and returns the spreadsheet ID as a string.

    Args:
        url_or_id (str): The ID or URL of the spreadsheet.

    Returns:
        str: The spreadsheet ID as a string.
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
    """
    Takes a URL to a Google spreadsheet and computes the spreadsheet ID from it according to the spreadsheet URL formula: https://docs.google.com/spreadsheets/d/<spreadsheet_id>/edit.
    If the URL is not formatted correctly, a ValueError will be raised.

    Args:
        url (str): The URL to the spreadsheet.

    Returns:
        str: The spreadsheet ID as a string.

    Raises:
        ValueError: If the URL is not properly formatted.
    """

    # split on the '/'
    parts = url.split("/")
    # loop through parts
    for i in range(len(parts)):
        # if we find
        if parts[i] == URL_ID_IDENTIFIER and i + 1 < len(parts):
            # if the id part is left empty then the url is not formatted correctly
            if parts[i + 1] == "":
                raise ValueError("Spreadsheet ID is an empty string")
            else:
                return parts[i + 1]
    # if url cannot be found, raise error
    raise ValueError("Invalid URL. Cannot find spreadsheet ID")


def get_first_rows(sheet_range: str) -> List[str]:
    """
    Receives the range of a Google sheet, parses it and outputs the sheet name, a range which includes the first 2 rows only.
    Is used for only getting the first 2 rows when collecting metadata.

    Args:
        sheet_range (str): Range of a Google sheet. Example: sheet1, sheet3!G18:O28. General formula {sheet_name}![Starting_column][Starting_row]:[Ending_column]:[Ending_row]

    Returns:
        List[str]: List containing the sheet name and the modified range to only have the first 2 rows.
    """

    # split on the !
    sheet_parts = sheet_range.split("!")
    sheet_name = sheet_parts[0]
    # the range can either have 1 or 2 parts: 1 part if it is simply a sheet name or 2 parts if it is an A1 range
    if len(sheet_parts) == 1:
        return [sheet_name, f"{sheet_name}!1:2"]
    elif len(sheet_parts) > 2:
        raise ValueError(
            "Range format is incorrect! Check documentation for correct usage."
        )

    range_name = sheet_parts[1]
    # split on the :, expecting strings in the form start:end, i.e 2 parts after splitting on the :
    # separate row and column letters from both the range start and end
    range_parts = range_name.split(":")
    if len(range_parts) != 2:
        raise ValueError(
            "Range format is incorrect! Check documentation for correct usage."
        )
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
    return [
        sheet_name,
        f"{sheet_name}!{starting_col}{starting_row}:{ending_col}{ending_row}",
    ]


def _separate_row_col(row_col_str: str) -> Tuple[str, str]:
    """
    Helper function that receives a row and column together from the A1 range and returns the row and column separately.

    Args:
        row_col_str (str): Row and column together from the A1 range. Example: "A1", "BB2", "ZZ25", etc.

    Returns:
        Tuple[str, str]: Row and column separately. Example: ("A", "1"), etc.
    """
    range_row = ""
    range_col = ""
    for range_char in row_col_str:
        if range_char.isdigit():
            range_row += range_char
        else:
            range_col += range_char
    return range_row, range_col


def convert_named_range_to_a1(
    named_range_dict: DictStrAny, sheet_names_dict: Dict[str, DictStrAny] = None
) -> str:
    """
    Converts a named_range dict returned from Google Sheets API metadata call to an A1 range.

    Args:
        named_range_dict (DictStrAny): Dict returned from Google Sheets API, containing information about a named range.
        sheet_names_dict (Dict[str, DictStrAny], optional): Dict containing all the sheets inside the spreadsheet where the sheet id is the key and the sheet name is the corresponding value.

    Returns:
        str: A string representing the named range as an A1 range.
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
    letters = [
        "",
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "I",
        "J",
        "K",
        "L",
        "M",
        "N",
        "O",
        "P",
        "Q",
        "R",
        "S",
        "T",
        "U",
        "V",
        "W",
        "X",
        "Y",
        "Z",
    ]
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
            col_name = letters[remainder + 1] + col_name
    return col_name or "A"


def get_range_headers(range_metadata: List[DictStrAny], range_name: str) -> List[str]:
    """
    Retrieves the headers for columns from the metadata of a range.

    Args:
        range_metadata (List[DictStrAny]): Metadata for the first 2 rows of a range.
        range_name (str): The name of the range as appears in the metadata.

    Returns:
        List[str]: A list of headers.
    """
    headers = []
    empty_header_index = 0
    for header in range_metadata[0]["values"]:
        if header:
            header_val = header["formattedValue"]
            # warn user when reading non string values as header - metadata is the only place to check this info
            if not ("stringValue" in header["effectiveValue"]):
                logger.warning(
                    f"In range {range_name}, header value: {header_val} is not a string! Name changed when loaded in database!"
                )
            headers.append(header_val)
        else:
            headers.append(f"empty_header_filler{empty_header_index}")
            empty_header_index = empty_header_index + 1
    # report if a header was empty
    if empty_header_index:
        logger.warning(
            f"In range {range_name}, {empty_header_index} headers were found empty!"
        )
    # manage headers being empty
    if len(headers) == empty_header_index:
        return []
    return headers


def get_first_line(range_metadata: List[DictStrAny]) -> List[bool]:
    """
    Determines if each column in the first line of a range contains datetime objects.

    Args:
        range_metadata (List[DictStrAny]): Metadata for the first 2 rows in a range.

    Returns:
        List[bool]: A list of boolean values indicating whether each column in the first line contains datetime objects.
    """

    # get data for 1st column and process them, if empty just return an empty list
    try:
        is_datetime_cols = is_date_datatype(range_metadata[1]["values"])
    except IndexError:
        return []
    return is_datetime_cols


def is_date_datatype(value_list: List[DictStrAny]) -> List[bool]:
    """
    Determines if each value in a list is a datetime object.

    Args:
        value_list (List[DictStrAny]): A list of values from the first row of data returned by Google Sheets API.

    Returns:
        List[bool]: A list of boolean values indicating whether each value is a datetime object.
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


def serial_date_to_datetime(
    serial_number: Union[int, float, str, bool]
) -> Union[pendulum.DateTime, str, bool]:
    """
    Converts a serial number to a datetime object.

    Args:
        serial_number (Union[int, float, str, bool]): The serial number, which can be an int, float, bool, or str.

    Returns:
        Union[pendulum.DateTime, str, bool]: The converted datetime object, or the original value if conversion fails.
    """

    # if called with a different data type, return with whatever input was, handled by the dlt pipeline later - edge case
    if not isinstance(serial_number, (int, float)):
        return serial_number
    # To get the seconds passed since the start date of serial numbers we round the product of the number of seconds in a day and the serial number
    conv_datetime: pendulum.DateTime = pendulum.from_timestamp(
        0, DLT_TIMEZONE
    ) + pendulum.duration(
        seconds=TIMESTAMP_CONST + round(SECONDS_PER_DAY * serial_number)
    )
    return conv_datetime


def metadata_preprocessing(
    ranges: List[str], named_ranges: DictStrAny = None
) -> Tuple[List[str], Dict[str, List[DictStrAny]]]:
    """
    Helper function that iterates through the input ranges and processes them so that only the first 2 rows are returned per range.
    It also structures all the ranges inside a dictionary similar to how they are returned by the Google Sheets API metadata request.

    Args:
        ranges (List[str]): List of range names.
        named_ranges (DictStrAny, optional): Dictionary containing ranges as keys and the corresponding named ranges as values.

    Returns:
        Tuple[List[str], Dict[str, List[DictStrAny]]]: A tuple containing:
            - meta_ranges: List containing all the ranges where metadata is gathered from.
            - response_like_dict: A dictionary that mirrors the structure of the Google Sheets API response.
              The keys are the parent sheets, and the values are lists of dictionaries containing metadata for each range.
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
        unfilled_range_dict = {
            "range": requested_range,
            "headers": [],
            "cols_is_datetime": [],
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


def process_range(
    sheet_val: List[List[Any]], sheet_meta: DictStrAny
) -> Iterator[DictStrAny]:
    """
    Receives 2 arrays of tabular data inside a sheet. This will be processed into a schema that is later stored into a database table.

    Args:
        sheet_val (List[List[Any]]): 2D array of values.
        sheet_meta (DictStrAny): Metadata gathered for this specific range.

    Yields:
        DictStrAny: A dictionary version of the table. It generates a dictionary of the type {header: value} for every row.
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
