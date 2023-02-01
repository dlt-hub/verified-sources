# Contains helper functions to make API calls

from typing import Any
import logging
import dlt
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.typing import DictStrAny
from dlt.common.exceptions import MissingDependencyException
from pipelines.google_sheets.data_validator import get_first_rows, is_date_datatype
try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google Sheets Source", ["google1", "google2"])


def api_auth(credentials: GcpClientCredentialsWithDefault) -> Resource:
    """
    Uses GCP credentials to authenticate with Google Sheets API
    @:param: credentials - credentials needed to log in to gcp
    @:return: service - object needed to make api calls to google sheets api
    """
    # Build the service object for Google sheets api.
    service = build("sheets", "v4", credentials=credentials.to_service_account_credentials())
    return service


def get_metadata_simple(spreadsheet_id: str, service: Resource) -> dict[DictStrAny]:
    """
    Makes a simple get metadata API call which just returns information about the spreadsheet such as: sheet_names and named_ranges
    @:param: spreadsheet_id - string containing the id of the spreadsheet
    @:param: service - Resource object used to make api calls to Google Sheets API
    @:param: get_sheets - setting: if true will return all sheets inside spreadsheet
    @:param: get_named_ranges - setting: if true will return all named ranges inside spreadsheet
    @:return: return_info - dict containing information on sheets inside if any and named ranges inside if any. Has 2 keys: "sheets" and "named_ranges"
    """
    return_info = {
        "sheets": {},
        "named_ranges": []
    }
    # get metadata of spreadsheet to check for number of sheets inside
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    # metadata["sheets"] is a list containing dicts with info on sheets inside the spreadsheet
    # iterate through the sheets in the metadata and get their names
    for sheet_m in metadata["sheets"]:
        # get name and append to list of sheet names
        sheet_name = sheet_m["properties"]["title"]
        sheet_id = sheet_m["properties"]["sheetId"]
        return_info["sheets"][sheet_id] = sheet_name
    logging.info(f"Found the following sheets {return_info['sheets']}")

    # this is a list containing dicts with info on named ranges
    if "namedRanges" in metadata:
        return_info["named_ranges"] = metadata["namedRanges"]
    logging.info(f"Found the following sheets {return_info['named_ranges']}")
    return return_info


def get_metadata(spreadsheet_id: str, service: Resource, ranges: list[str], named_ranges: dict[str] = None) -> dict[Any]:
    """
    # TODO: add fields to save on info returned
    Gets the metadata for the first 2 rows of every range specified. The first row is deduced as the header and the 2nd row specifies the format the rest of the data should follow
    @:param: spreadsheet_id - the id of the spreadsheet
    @:param: service - Resource object used by google-api-python-client to make api calls
    @:param: ranges - List of ranges to get data from. If left empty, every sheet inside the spreadsheet will be included instead. named ranges not supported
    @:return: ranges_data - A dict where all the range names are the key. The values for each key are the corresponding sheet metadata: sheet_name, headers, values
    """

    # process metadata ranges so only the first 2 rows are appended
    # response like dict will contain a dict similar to the response by the Google Sheets API: ranges are returned inside the sheets they belong in the order given in the API request.
    meta_ranges = []
    response_like_dict = {}
    metadata_all_ranges = {}
    for requested_range in ranges:
        # convert range to first 2 rows
        range_info = get_first_rows(requested_range)
        sheet_name = range_info[0]
        unfilled_range_dict = {"range": requested_range,
                               "headers": [],
                               "cols_is_datetime": [],
                               "name": None
                               }
        # check whether this range is a named range and save info in metadata if so
        if named_ranges and requested_range in named_ranges:
            unfilled_range_dict["name"] = named_ranges[requested_range]
        # add a dict with range info to the list of ranges inside the sheet
        if sheet_name in response_like_dict:
            response_like_dict[sheet_name].append(unfilled_range_dict)
        else:
            response_like_dict[sheet_name] = [unfilled_range_dict]
        meta_ranges.append(range_info[1])

    # make call to get metadata
    # TODO: add fields so the calls are more efficient
    spr_meta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=meta_ranges,
        includeGridData=True
    ).execute()

    # process and populate metadata in response like dict
    for sheet in spr_meta["sheets"]:
        # get sheet name, so we can associate with dict and load the data into dict
        meta_sheet_name = sheet["properties"]["title"]
        sheet_data = sheet["data"]

        # skip record if not found in the response dict
        if not (meta_sheet_name in response_like_dict):
            continue

        # get ranges inside the sheet in order
        for i in range(len(sheet_data)):
            # check that sheet is not empty, otherwise delete
            if not ("rowData" in sheet_data[i]):
                logging.warning(f"Metadata - Skipped empty range: {response_like_dict[meta_sheet_name][i]['range']}")
                continue
            # get headers and 1st row data
            range_metadata = sheet_data[i]["rowData"]
            headers = _get_range_headers(range_metadata=range_metadata)
            if not headers:
                logging.warning(f"Metadata: Skipped range with empty headers: {response_like_dict[meta_sheet_name][i]['range']}")
                continue
            first_line_values = _get_first_line(range_metadata=range_metadata)
            if not first_line_values:
                logging.warning(f"Metadata: No data values for the first line of data {response_like_dict[meta_sheet_name][i]['range']}")

            # add headers and values
            response_like_dict[meta_sheet_name][i]["headers"] = headers
            response_like_dict[meta_sheet_name][i]["cols_is_datetime"] = first_line_values

            # append dict to response
            metadata_range_name = response_like_dict[meta_sheet_name][i]["range"]
            metadata_all_ranges[metadata_range_name] = response_like_dict[meta_sheet_name][i]
    return metadata_all_ranges


def _get_range_headers(range_metadata: dict[Any]) -> list[str]:
    """
    Helper. Receives the metadata for a range of cells and outputs only the headers for columns, i.e first line of data
    @:param: range_metadata - Dict containing metadata for the first 2 rows of a range, taken from Google Sheets API response.
    @:return: headers - list of headers
    """
    headers = []
    empty_header_index = 0
    for header in range_metadata[0]["values"]:
        if header:
            headers.append(header["formattedValue"])
        else:
            headers.append(f"empty_header_filler{empty_header_index}")
            empty_header_index = empty_header_index + 1
    # manage headers being empty
    if len(headers) == empty_header_index:
        return []
    return headers


def _get_first_line(range_metadata: str) -> list[bool]:
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
