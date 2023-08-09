"""Contains helper functions to extract data from spreadsheet API"""

from typing import Any, List, Tuple

from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import DictStrAny

from dlt.sources.credentials import GcpCredentials, GcpOAuthCredentials

from .data_processing import ParsedRange, trim_range_top_left

try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google API Client", ["google-api-python-client"])


def api_auth(credentials: GcpCredentials) -> Resource:
    """
    Uses GCP credentials to authenticate with Google Sheets API.

    Args:
        credentials (GcpCredentials): Credentials needed to log in to GCP.

    Returns:
        Resource: Object needed to make API calls to Google Sheets API.
    """
    if isinstance(credentials, GcpOAuthCredentials):
        credentials.auth("https://www.googleapis.com/auth/spreadsheets.readonly")
    # Build the service object for Google sheets api.
    service = build("sheets", "v4", credentials=credentials.to_native_credentials())
    return service


def get_known_range_names(
    spreadsheet_id: str, service: Resource
) -> Tuple[List[str], List[str], str]:
    """
    Retrieves spreadsheet metadata and extracts a list of sheet names and named ranges

    Args:
        spreadsheet_id (str): The ID of the spreadsheet.
        service (Resource): Resource object used to make API calls to Google Sheets API.

    Returns:
        Tuple[List[str], List[str], str] sheet names, named ranges, spreadheet title
    """
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_names: List[str] = [s["properties"]["title"] for s in metadata["sheets"]]
    named_ranges: List[str] = [r["name"] for r in metadata.get("namedRanges", {})]
    title: str = metadata["properties"]["title"]
    return sheet_names, named_ranges, title


def get_data_for_ranges(
    service: Resource, spreadsheet_id: str, range_names: List[str]
) -> List[Tuple[str, ParsedRange, ParsedRange, List[List[Any]]]]:
    """
    Calls Google Sheets API to get data in a batch. This is the most efficient way to get data for multiple ranges inside a spreadsheet.

    Args:
        service (Resource): Object to make API calls to Google Sheets.
        spreadsheet_id (str): The ID of the spreadsheet.
        range_names (List[str]): List of range names.

    Returns:
        List[DictStrAny]: A list of ranges with data in the same order as `range_names`
    """
    range_batch_resp = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=range_names,
            # un formatted returns typed values
            valueRenderOption="UNFORMATTED_VALUE",
            # will return formatted dates as a serial number
            dateTimeRenderOption="SERIAL_NUMBER",
        )
        .execute()
    )
    # if there are not ranges to be loaded, there's no "valueRanges"
    range_batch: List[DictStrAny] = range_batch_resp.get("valueRanges", [])
    # trim the empty top rows and columns from the left
    rv = []
    for name, range_ in zip(range_names, range_batch):
        parsed_range = ParsedRange.parse_range(range_["range"])
        values: List[List[Any]] = range_.get("values", None)
        if values:
            parsed_range, values = trim_range_top_left(parsed_range, values)
        # create a new range to get first two rows
        meta_range = parsed_range._replace(end_row=parsed_range.start_row + 1)
        # print(f"{name}:{parsed_range}:{meta_range}")
        rv.append((name, parsed_range, meta_range, values))
    return rv
