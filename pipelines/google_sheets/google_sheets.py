# Contains the main pipeline functions

from typing import Any, Iterator, cast
import dlt
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.typing import DictStrAny, TDataItem
from dlt.common.exceptions import MissingDependencyException
import logging
from pipelines.google_sheets.data_validator import serial_date_to_datetime, get_spreadsheet_id, get_first_rows, get_data_type

try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google Sheets Source", ["google1", "google2"])

# TODO: consider using https://github.com/burnash/gspread for spreadsheet discovery
# TODO: add exceptions to the code in case HTTP requests fail


def get_metadata(spreadsheet_id: str, service: Resource, ranges: list[str]) -> Iterator[DictStrAny]:
    """
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
    spr_meta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=ranges,
        includeGridData=True
    ).execute()

    # process metadata, loop through every sheet that is returned from the given ranges
    for sheet in spr_meta['sheets']:

        # get sheet data
        meta_sheet_name = sheet['properties']['title']
        sheet_range_data = sheet['data']

        # a sheet can have 1 or more ranges in it
        # the order of the ranges is preserved by how they are given in the ranges parameter
        for i in range(len(sheet_range_data)):
            # get the sheet range as it appears in the api response
            sh_range = sheet_range_data[i]

            # Combine sheet name and index to function as a key for the ranges dict
            range_id = meta_sheet_name + str(i)

            # key does not exist in ranges
            if not (range_id in find_ranges):
                break
            # key exists
            my_range = find_ranges[range_id]

            # get headers and 1st row data
            headers = []
            for header in sh_range['rowData'][0]['values']:
                headers.append(header['formattedValue'])

            # get data types for the first row
            first_line_values = get_data_type(sh_range['rowData'][1]['values'])

            # ensure there are no empty headers
            if len(first_line_values) > len(headers):
                raise ValueError("Data has more values than headers")

            # add headers and values
            ranges_data[my_range]["headers"] = headers
            ranges_data[my_range]["values"] = first_line_values
    return ranges_data


def process_range(sheet_val: Iterator[DictStrAny], sheet_meta: Iterator[DictStrAny]) -> Iterator[DictStrAny]:
    """
    This function receives 2 arrays of tabular data inside a sheet. This will be processed into a schema that is later stored into a database table
    @:param: spreadsheet_id - the id of the spreadsheet
    @:param: spreadsheets_resource - Resource object, used to make calls to google spreadsheets
    @:return:  ?
    """
    # get headers and first line data types which is just Datetime or not datetime so far
    headers = sheet_meta['headers']
    first_line_val_types = sheet_meta['values']
    for row in sheet_val[1:]:
        table_dict = {}

        # empty row
        if not row:
            break

        # process both rows and check for differences to spot dates
        for val, header, is_datetime in zip(row, headers, first_line_val_types):
            # check whether the object is a datetime object
            if is_datetime:
                converted_sn = serial_date_to_datetime(val)
                table_dict[header] = converted_sn
            else:
                # just input the regular value
                table_dict[header] = val
        logging.info("Finished loading table")
        yield table_dict


def _initialize_sheets(credentials: GcpClientCredentialsWithDefault) -> Any:
    """
    Helper function, uses GCP credentials to authenticate with Google Sheets API
    @:param: credentials - credentials needed to log in to gcp
    @:return: service - object needed to make api calls to google sheets api
    """
    # Build the service object for Google sheets api.
    service = build('sheets', 'v4', credentials=credentials.to_service_account_credentials())
    return service


@dlt.source
def google_spreadsheet(spreadsheet_identifier: str, sheet_names: list[str] = None, credentials: GcpClientCredentialsWithDefault = dlt.secrets.value, batch_size: int = 0) -> Any:
    """
    This is the source for the dlt pipeline. It returns the following resources: 1 dlt resource for every range in sheet_names
    @:param: spreadsheet_identifier - the id or url to the spreadsheet
    @:param: sheet_names - A list of ranges in the spreadsheet of the type sheet_name!range_name. These are the ranges to be converted into tables
    @:param: credentials - GCP credentials to the account with Google Sheets api access, defined in dlt.secrets
    @:param: batch_size - the size of batches when retrieving data, default is 0
    @:return: multiple dlt resources
    """

    # authenticate to the service using the helper function
    service = _initialize_sheets(cast(GcpClientCredentialsWithDefault, credentials))
    logging.info("Successful Authentication")

    # get spreadsheet id from url or id
    spreadsheet_id = get_spreadsheet_id(spreadsheet_identifier)

    # if sheet names is left empty, make an api call and discover all the sheets in the spreadsheet
    if not sheet_names:
        logging.info("No ranges provided. Scouting for ranges")
        sheet_names = []
        # get metadata of spreadsheet to check for number of sheets inside
        metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_meta = metadata['sheets']
        # iterate through the sheets in the metadata and get their names
        for sheet_m in sheet_meta:
            # get name and append to list of sheet names
            sheet_name = sheet_m['properties']['title']
            sheet_names.append(sheet_name)
        logging.info(f"Found the following sheets {sheet_names}")

    # get metadata on the first 2 rows for every provided range
    metadata_ranges_all = get_metadata(spreadsheet_id=spreadsheet_id, service=service, ranges=sheet_names)

    # Make an api call to get the data for all sheets and ranges
    # get dates as serial number
    values = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=sheet_names,
        # unformated returns typed values
        valueRenderOption="UNFORMATTED_VALUE",
        # will return formatted dates as a serial number
        dateTimeRenderOption="SERIAL_NUMBER"
    ).execute()['valueRanges']
    logging.info("Data fetched")

    # iterate through values
    for i in range(len(values)):
        # get range names and values
        sheet_range_name = values[i]['range']
        sheet_range = values[i]['values']

        # get metadata for sheet
        if sheet_range_name in metadata_ranges_all:
            sheet_meta_batch = metadata_ranges_all[sheet_range_name]
        else:
            # if range doesn't exist as a key then it means the key is just the sheet name and google sheets api just filled the range
            sheet_name_batch = sheet_range_name.split("!")[0]
            sheet_meta_batch = metadata_ranges_all[sheet_name_batch]
        # create a resource from processing both sheet/range values
        yield dlt.resource(process_range(sheet_val=sheet_range, sheet_meta=sheet_meta_batch),
                           name=sheet_range_name,
                           write_disposition="replace")
    logging.info("Finished loading all ranges")

    yield (
        metadata_table(spreadsheet_info=metadata_ranges_all, spreadsheet_id=spreadsheet_id)
    )
    logging.info("Finished loading metadata table")


@dlt.resource(write_disposition="replace", name="spreadsheet_info")
def metadata_table(spreadsheet_info: Iterator[DictStrAny], spreadsheet_id: str) -> Iterator[TDataItem]:
    """
    This is the metadata_table resource. It adds a table with all loaded ranges into a table
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
        range_num_headers = len(loaded_range_meta['headers'])
        range_sheet_name = loaded_range_meta['sheet_name']

        # table structure
        table_dict = {
            "spreadsheet_id": spreadsheet_id,
            "loaded_range": loaded_range,
            "sheet_name": range_sheet_name,
            "num_headers": range_num_headers
        }
        yield table_dict
