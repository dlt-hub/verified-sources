"""Contains helper functions to make API calls"""

from typing import List

from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import DictStrAny

from dlt.sources.credentials import GcpCredentials, GcpOAuthCredentials

from .data_processing import get_first_line, get_range_headers, metadata_preprocessing

try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google API Client", ["google-api-python-client"])


def api_auth(credentials: GcpCredentials) -> Resource:
    """
    Uses GCP credentials to authenticate with Google Sheets API
    @:param: credentials - credentials needed to log in to gcp
    @:return: service - object needed to make api calls to google sheets api
    """
    if isinstance(credentials, GcpOAuthCredentials):
        credentials.auth("https://www.googleapis.com/auth/spreadsheets.readonly")
    # Build the service object for Google sheets api.
    service = build("sheets", "v4", credentials=credentials.to_native_credentials())
    return service


def get_metadata_simple(spreadsheet_id: str, service: Resource) -> DictStrAny:
    """
    Makes a simple get metadata API call which just returns information about the spreadsheet such as: sheet_names and named_ranges
    @:param: spreadsheet_id - string containing the id of the spreadsheet
    @:param: service - Resource object used to make api calls to Google Sheets API
    @:param: get_sheets - setting: if true will return all sheets inside spreadsheet
    @:param: get_named_ranges - setting: if true will return all named ranges inside spreadsheet
    @:return: return_info - dict containing information on sheets inside if any and named ranges inside if any. Has 2 keys: "sheets" and "named_ranges"
    """
    return_info: DictStrAny = {
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
    logger.info(f"Found the following sheets {return_info['sheets']}")

    # this is a list containing dicts with info on named ranges
    if "namedRanges" in metadata:
        return_info["named_ranges"] = metadata["namedRanges"]
    logger.info(f"Found the following sheets {return_info['named_ranges']}")
    return return_info


def get_metadata(spreadsheet_id: str, service: Resource, ranges: List[str], named_ranges: DictStrAny = None) -> DictStrAny:
    """
    # TODO: add fields to save on info returned
    Gets the metadata for the first 2 rows of every range specified. The first row is deduced as the header and the 2nd row specifies the format the rest of the data should follow
    @:param spreadsheet_id: - the id of the spreadsheet
    @:param service: - Resource object used by google-api-python-client to make api calls
    @:param ranges: - List of ranges to get data from. If left empty, every sheet inside the spreadsheet will be included instead. named ranges not supported
    @:param named_ranges: Dict containing ranges as keys and the corresponding named ranges as the values
    @:returns: - A dict where all the range names are the key. The values for each key are the corresponding sheet metadata: sheet_name, headers, values
    """

    # process metadata ranges so only the first 2 rows are appended
    # response like dict will contain a dict similar to the response by the Google Sheets API: ranges are returned inside the sheets they belong in the order given in the API request.
    meta_ranges, response_like_dict = metadata_preprocessing(ranges=ranges, named_ranges=named_ranges)
    spr_meta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=meta_ranges,
        includeGridData=True
    ).execute()

    # process and populate metadata in response like dict but return in from metadata_all_ranges because we need the data returned in a more organized format
    metadata_all_ranges = {}
    for sheet in spr_meta["sheets"]:
        # get sheet name, so we can associate with dict and load the data into dict
        meta_sheet_name = sheet["properties"]["title"]
        sheet_data = sheet["data"]

        # skip record if not found in the response dict
        if not (meta_sheet_name in response_like_dict):
            continue
        # get ranges inside the sheet in order
        for i in range(len(sheet_data)):
            metadata_range_name = response_like_dict[meta_sheet_name][i]["range"]
            # check that sheet is not empty, otherwise delete
            if not ("rowData" in sheet_data[i]):
                logger.warning(f"Metadata - Skipped empty range: {metadata_range_name}")
                continue
            # get headers and 1st row data
            range_metadata = sheet_data[i]["rowData"]
            headers = get_range_headers(range_metadata=range_metadata, range_name=metadata_range_name)
            if not headers:
                logger.warning(f"Metadata: Skipped range with empty headers: {metadata_range_name}")
                continue
            first_line_values = get_first_line(range_metadata=range_metadata)
            if not first_line_values:
                logger.warning(f"Metadata: No data values for the first line of data {metadata_range_name}")
            # add headers and values
            response_like_dict[meta_sheet_name][i]["headers"] = headers
            response_like_dict[meta_sheet_name][i]["cols_is_datetime"] = first_line_values
            # append dict to response
            metadata_all_ranges[metadata_range_name] = response_like_dict[meta_sheet_name][i]
    return metadata_all_ranges


def get_data_batch(service: Resource, spreadsheet_id: str, range_names: List[str]) -> List[DictStrAny]:
    """
    Calls Google Sheets API to get data in a batch. This is the most efficient way to get data for multiple ranges inside a spreadsheet. However, this API call will return the data for each range
    without the same name that the range was called
    @:param: service - Object to make api calls to Google Sheets
    @:param: spredsheet_id - the id of the spreadsheet
    @:param: range_names - list of range names
    @:return: values - list of dictionaries, each dictionary will contain all data for one of the requested ranges
    """
    # handle requests with no ranges - edge case
    if not range_names:
        logger.warning("Fetching data error: No ranges to get data from. Check the input ranges are not empty.")
        return []
    # Make an api call to get the data for all sheets and ranges
    # get dates as serial number
    values: List[DictStrAny] = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=range_names,
        # un formatted returns typed values
        valueRenderOption="UNFORMATTED_VALUE",
        # will return formatted dates as a serial number
        dateTimeRenderOption="SERIAL_NUMBER"
    ).execute()["valueRanges"]
    logger.info("Data fetched")
    return values
