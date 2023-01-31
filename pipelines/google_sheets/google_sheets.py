# Contains the main pipeline functions

from typing import Any, Iterator, cast
import dlt
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.typing import DictStrAny, TDataItem
from dlt.common.exceptions import MissingDependencyException
import logging
from pipelines.google_sheets.data_validator import serial_date_to_datetime, get_spreadsheet_id, get_first_rows, is_date_datatype, convert_named_range_to_a1
from pipelines.google_sheets.api_calls import api_auth
from pipelines.google_sheets import api_calls
try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google Sheets Source", ["google1", "google2"])

# TODO: add exceptions to the code in case HTTP requests fail


def get_metadata(spreadsheet_id: str, service: Resource, ranges: list[str]) -> dict[Any]:
    """
    # TODO: add fields to save on info returned
    Gets the metadata for the first 2 rows of every range specified. The first row is deduced as the header and the 2nd row specifies the format the rest of the data should follow
    @:param: spreadsheet_id - the id of the spreadsheet
    @:param: service - Resource object used by google-api-python-client to make api calls
    @:param: ranges - List of ranges to get data from. If left empty, every sheet inside the spreadsheet will be included instead. named ranges not supported
    @:return: ranges_data - A dict where all the range names are the key. The values for each key are the corresponding sheet metadata: sheet_name, headers, values
    """

    # process metadata ranges so only the first 2 rows are appended
    ranges_data = {}
    sheet_indexing = {}
    meta_ranges = []
    find_ranges = {}
    for sh_range in ranges:
        # convert range to first 2 rows
        # TODO: find another way to parse ranges
        range_info = get_first_rows(sh_range)
        sheet_name = range_info[0]

        # Google Sheets API will respond with sheet data in order and ranges inside each belonging sheet. Since the range name is not returned in the response, we need sheet name
        # and an index to serve as a unique identifier for each sheet. So a dict is made with [sheet+index] as key and as the range name as a value
        if sheet_name in sheet_indexing:
            range_index = sheet_indexing[sheet_name] + 1
        else:
            range_index = 0
        sheet_indexing[sheet_name] = range_index

        # append to ranges dict
        range_dict = {
            "headers": [],
            "values": [],
            "sheet_name": sheet_name,
            "index": range_index
        }
        ranges_data[sh_range] = range_dict
        find_ranges[f"{sheet_name}{range_index}"] = sh_range

        # get first 2 rows range and then add to a list for the api call
        first_rows_range = range_info[1]
        meta_ranges.append(first_rows_range)

    # make call to get metadata
    # TODO: add fields so the calls are more efficient
    spr_meta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=meta_ranges,
        includeGridData=True
    ).execute()

    # process metadata, loop through every sheet that is returned from the given ranges
    for sheet in spr_meta["sheets"]:

        # get sheet data
        meta_sheet_name = sheet["properties"]["title"]
        sheet_range_data = sheet["data"]

        # a sheet can have 1 or more ranges in it
        # the order of the ranges is preserved by how they are given in the ranges parameter
        for i in range(len(sheet_range_data)):
            # get the sheet range as it appears in the api response
            sh_range = sheet_range_data[i]

            # Combine sheet name and index to function as a key for the ranges dict
            range_id = meta_sheet_name + str(i)

            # key does not exist in ranges
            if not (range_id in find_ranges):
                continue

            # key exists
            my_range = find_ranges[range_id]

            # check sheet is not empty
            try:
                row_data_range = sh_range["rowData"]
            except KeyError:
                logging.warning(f"Metadata: Skipped empty range: {my_range}")
                del ranges_data[my_range]
                continue

            # get headers and 1st row data
            headers = []
            empty_header_index = 0
            for header in row_data_range[0]["values"]:
                if header:
                    headers.append(header["formattedValue"])
                else:
                    headers.append(f"empty_header_filler{empty_header_index}")
                    empty_header_index = empty_header_index + 1

            # manage headers being empty
            if len(headers) == empty_header_index:
                logging.warning(f"Metadata: Skipped deformed range: {my_range}")
                del ranges_data[my_range]
                continue

            try:
                # get data types for the first row
                first_line_values = is_date_datatype(row_data_range[1]["values"])
            except IndexError:
                first_line_values = []
                logging.warning(f"Metadata: No data values for the first line of data {my_range}")

            # add headers and values
            ranges_data[my_range]["headers"] = headers
            ranges_data[my_range]["values"] = first_line_values
    return ranges_data


@dlt.source
def google_spreadsheet(spreadsheet_identifier: str, range_names: list[str] = None , credentials: GcpClientCredentialsWithDefault = dlt.secrets.value,
                       get_sheets: bool = True, get_named_ranges: bool = True) -> Any:
    """
    The source for the dlt pipeline. It returns the following resources: 1 dlt resource for every range in sheet_names
    @:param: spreadsheet_identifier - the id or url to the spreadsheet
    @:param: sheet_names - A list of ranges in the spreadsheet of the type sheet_name!range_name. These are the ranges to be converted into tables
    @:param: credentials - GCP credentials to the account with Google Sheets api access, defined in dlt.secrets
    @:return: multiple dlt resources
    """

    # authenticate to the service using the helper function
    service = api_auth(cast(GcpClientCredentialsWithDefault, credentials))
    logging.info("Successful Authentication")

    # get spreadsheet id from url or id
    spreadsheet_id = get_spreadsheet_id(spreadsheet_identifier)

    # if range_names were not provided, initialize them as an empty list
    if not range_names:
        range_names = []
    # if sheet names or named_ranges are to be added as tables, an extra api call is made and
    if get_sheets or get_named_ranges:
        # get metadata and append to list of ranges as needed
        simple_metadata = api_calls.get_metadata_simple(spreadsheet_id=spreadsheet_id, service=service)
        if get_sheets:
            range_names += list(simple_metadata["sheets"].values())
        if get_named_ranges:
            named_ranges = {convert_named_range_to_a1(named_range_dict=named_range, sheet_names_dict=simple_metadata["sheets"]): named_range["name"] for named_range in simple_metadata["named_ranges"]}
            range_names += list(named_ranges.keys())
    # get metadata on the first 2 rows for every provided range
    metadata_ranges_all = get_metadata(spreadsheet_id=spreadsheet_id, service=service, ranges=range_names)
    data_resources_list = get_data(service=service, spreadsheet_id=spreadsheet_id, range_names=range_names, metadata_dict=metadata_ranges_all)
    metadata_resource = metadata_table(spreadsheet_info=metadata_ranges_all, spreadsheet_id=spreadsheet_id)
    data_resources_list.append(metadata_resource)
    return data_resources_list


@dlt.resource(write_disposition="replace", name="spreadsheet_info")
def metadata_table(spreadsheet_info: dict[Any], spreadsheet_id: str) -> Iterator[TDataItem]:
    """
    Creates the metadata_table resource. It adds a table with all loaded ranges into a table
    @:param: spreadsheet_info - This is a dict where all loaded ranges are keys. Inside the dict there is another dict with keys: "headers", "sheet_name", "index" and "values"
    @:param: spreadsheet_id - the id of the spreadsheet is included for extra info
    @:returns: DLT table
    """

    # get keys for metadata dict and iterate through them
    # the keys for this dict are the ranges where the data is gathered from
    loaded_ranges = spreadsheet_info.keys()
    for loaded_range in loaded_ranges:

        # get needed info from dict
        loaded_range_meta = spreadsheet_info[loaded_range]
        range_num_headers = len(loaded_range_meta["headers"])
        range_sheet_name = loaded_range_meta["sheet_name"]

        # table structure
        table_dict = {
            "spreadsheet_id": spreadsheet_id,
            "loaded_range": loaded_range,
            "sheet_name": range_sheet_name,
            "num_cols": range_num_headers
        }
        yield table_dict


def get_data(service: Resource, spreadsheet_id: str, range_names: list[str], metadata_dict: dict[DictStrAny]) -> list[Iterator[Any]]:
    """
    Makes an api call to Google sheets and retrieve all the ranges listed in range_names and process them into dlt resources
    @:param: service - Object to make api calls to Google Sheets
    @:param: spredsheet_id - the id of the spreadsheet
    @:param: range_names - list of range names
    @:param: metadata_dict - the dict with metadata
    @:returns: my_resources - list of dlt resources, each containing a table of a specific range
    """

    my_resources = []
    # Make an api call to get the data for all sheets and ranges
    # get dates as serial number
    values = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=range_names,
        # un formatted returns typed values
        valueRenderOption="UNFORMATTED_VALUE",
        # will return formatted dates as a serial number
        dateTimeRenderOption="SERIAL_NUMBER"
    ).execute()["valueRanges"]
    logging.info("Data fetched")

    # iterate through values
    for i in range(len(values)):
        # get range name
        sheet_range_name = values[i]["range"]

        # get metadata for sheet
        if sheet_range_name in metadata_dict:
            sheet_meta_batch = metadata_dict[sheet_range_name]
        else:
            # if range doesn't exist as a key then it means the key is just the sheet name and google sheets api just filled the range
            sheet_range_name = sheet_range_name.split("!")[0]
            try:
                sheet_meta_batch = metadata_dict[sheet_range_name]
            except KeyError:
                # sheet is not there because it was popped
                logging.warning(f"Skipping data for empty range: {sheet_range_name}")
                continue

        # get range values
        sheet_range = values[i]["values"]
        # create a resource from processing both sheet/range values
        my_resources.append(dlt.resource(process_range(sheet_val=sheet_range, sheet_meta=sheet_meta_batch),
                                         name=sheet_range_name,
                                         write_disposition="replace")
                            )
        logging.info(f"Data for {sheet_range_name} loaded")
    logging.info("Finished loading all ranges")
    return my_resources


def process_range(sheet_val: Iterator[DictStrAny], sheet_meta: dict[Any]) -> Iterator[DictStrAny]:
    """
    Receives 2 arrays of tabular data inside a sheet. This will be processed into a schema that is later stored into a database table
    @:param: spreadsheet_id - the id of the spreadsheet
    @:param: spreadsheets_resource - Resource object, used to make calls to google spreadsheets
    @:return:  ?
    """
    # get headers and first line data types which is just Datetime or not datetime so far
    headers = sheet_meta["headers"]
    first_line_val_types = sheet_meta["values"]

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
        print(table_dict)
        yield table_dict
