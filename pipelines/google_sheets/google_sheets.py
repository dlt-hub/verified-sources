# Contains the main pipeline functions

from typing import Any, Iterator, cast
import dlt
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.typing import DictStrAny
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItems
import logging
import time
from pipelines.google_sheets.data_validator import process_url, serial_date_to_datetime, get_spreadsheet_id

try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google Sheets Source", ["google1", "google2"])

METADATA_SEARCH_FIELD = 'sheets(data(rowData(values)))'

# TODO: consider using https://github.com/burnash/gspread for spreadsheet discovery
# TODO: add exceptions to the code in case HTTP requests fail


def process_range(sheet_val: Iterator[DictStrAny], sheet_val_sn: Iterator[DictStrAny]) -> Iterator[DictStrAny]:
    """
    This function receives 2 arrays of tabular data inside a sheet. This will be processed into a schema that is later stored into a database table
    @:param: spreadsheet_id - the id of the spreadsheet
    @:param: spreadsheets_resource - Resource object, used to make calls to google spreadsheets
    @:return:  ?
    """
    # get headers - same for both arrays
    headers = sheet_val[0]  # list
    for row, row_sn in zip(sheet_val[1:], sheet_val_sn[1:]):
        table_dict = {}
        # process both rows and check for differences to spot dates
        for val, val_sn, header in zip(row, row_sn, headers):
            # check that they have different types: val-str and val_sn is either an int or float and then process the dates
            if isinstance(val, str) and isinstance(val_sn, (int, float)):
                # if type is different we have a date value
                # process the value
                converted_sn = serial_date_to_datetime(val_sn)
                table_dict[header] = converted_sn
            else:
                # just input the regular value
                table_dict[header] = val
        print(table_dict)
        yield table_dict


#
def _initialize_sheets(credentials: GcpClientCredentialsWithDefault) -> Any:
    """"
    Helper function, uses GCP credentials to authenticate with Google Sheets API
    @:param: credentials - credentials needed to login to gcp
    @:return: service - object needed to make api calls to google sheets api
    """
    # Build the service object for Google sheets api.
    service = build('sheets', 'v4', credentials=credentials.to_service_account_credentials())
    return service


@dlt.source
def google_spreadsheet(spreadsheet_identifier: str, sheet_names: list[str] = None, credentials: GcpClientCredentialsWithDefault = dlt.secrets.value) -> Any:
    """"
    This is the source for the dlt pipeline. It returns the following resources: 1 dlt resource for every range in sheet_names
    @:param: spreadsheet_identifier - the id or url to the spreadsheet
    @:param: sheet_names - A list of ranges in the spreadsheet of the type sheet_name!range_name. These are the ranges to be converted into tables
    @:param: credentials - GCP credentials to the account with Google Sheets api access, defined in dlt.secrets
    @:return: multiple dlt resources
    """

    # authenticate to the service using the helper function
    service = _initialize_sheets(cast(GcpClientCredentialsWithDefault, credentials))
    sheet = service.spreadsheets()

    # get spreadsheet id from url or id
    spreadsheet_id = get_spreadsheet_id(spreadsheet_identifier)

    # if sheet names is left empty, make an api call and discover all the sheets in the spreadsheeet
    if not sheet_names:
        sheet_names = []

        # get metadata of spreadsheet to check for number of sheets inside
        metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_meta = metadata['sheets']
        # iterate through the sheets in the metadata and get their names
        for sheet_m in sheet_meta:
            # get name and append to list of sheet names
            sheet_name = sheet_m['properties']['title']
            sheet_names.append(sheet_name)

    # Make 2 api calls to get the data for all sheets and ranges
    # Get values where dates are a string and get values where dates are a serial number, this is done so the fields which are dates can be distinguished and properly calculated
    # get values as a string
    values = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=sheet_names,
        # unformatted returns typed values
        valueRenderOption="UNFORMATTED_VALUE",
        # will return formatted dates as string
        dateTimeRenderOption="FORMATTED_STRING"
    ).execute()['valueRanges']

    # get dates as serial number
    values_date_sn = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=sheet_names,
        # unformatted returns typed values
        valueRenderOption="UNFORMATTED_VALUE",
        # will return formatted dates as a serial number
        dateTimeRenderOption="SERIAL_NUMBER"
    ).execute()['valueRanges']

    # iterate through both sets of values, same indexes work
    for i in range(len(values)):
        # get range names and values
        sheet_range_name = values[i]['range']
        sheet_range = values[i]['values']
        sheet_range_sn = values_date_sn[i]['values']

        # create a resource from processing both sheet/range values
        yield dlt.resource(process_range(sheet_val=sheet_range, sheet_val_sn=sheet_range_sn),
                           name=sheet_range_name,
                           write_disposition="replace")