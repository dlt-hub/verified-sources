# Contains the main pipeline functions

from typing import Any, Iterator, cast
import dlt
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.typing import DictStrAny
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItems
import logging
import time

try:
    from data_validator import serial_date_to_datetime, process_url
except ModuleNotFoundError:
    from google_sheets.data_validator import process_url, serial_date_to_datetime

try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google Sheets Source", ["google1", "google2"])

# constants
SPREADSHEET_ID = "1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1NVxFQYRqrGmur_MeIc4m4ewToF802uy2ObC61HOCstU/edit#gid=0"


# gets sheet values from a single spreadsheet preserving original typing for schema generation
# TODO: write schema discovery to better handle dates
# TODO: consider using https://github.com/burnash/gspread for spreadsheet discovery
# TODO: add exceptions to the code in case HTTP requests fail

# this function receives 2 arrays of tabular data inside a sheet. This will be processed into a schema that is later stored into a database table
# it will then
def process_range(sheet_val: Iterator[DictStrAny], sheet_val_sn: Iterator[DictStrAny]) -> Iterator[DictStrAny]:

    # get headers - same for both arrays
    headers = sheet_val[0]  # list
    for row, row_sn in zip(sheet_val[1:], sheet_val_sn[1:]):
        table_dict = {}
        # process both rows and check for differences to spot dates
        for val, val_sn, header in zip(row, row_sn, headers):
            # check that they have different types and then process the dates
            if type(val) != type(val_sn) and (isinstance(val_sn, int) or isinstance(val_sn, float)):
                # if type is different we have a date value
                # process the value
                converted_sn = serial_date_to_datetime(val_sn)
                table_dict[header] = converted_sn
            else:
                # just input the regular value
                table_dict[header] = val
        print(table_dict)
        yield table_dict


# helper function, uses GCP credentials to authenticate with Google Sheets API
def _initialize_sheets(credentials: GcpClientCredentialsWithDefault) -> Any:
    # Build the service object for Google sheets api.
    service = build('sheets', 'v4', credentials=credentials.to_service_account_credentials())
    return service


@dlt.source
def google_spreadsheet(spreadsheet_id: str = None, spreadsheet_url: str = None, sheet_names: list[str] = None, credentials: GcpClientCredentialsWithDefault = dlt.secrets.value) -> Any:

    # authenticate to the service using the helper function
    service = _initialize_sheets(cast(GcpClientCredentialsWithDefault, credentials))
    sheet = service.spreadsheets()

    # get the id from url if the id was left empty
    if not spreadsheet_id:
        spreadsheet_id = process_url(spreadsheet_url)

    # if sheet names is left empty, make an api call and discover all the sheets in the spreadsheeet
    if (not sheet_names) or len(sheet_names) == 0:
        sheet_names = []

        # get metadata of spreadsheet to check for number of sheets inside
        metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
        sheet_meta = metadata['sheets']
        # iterate through the sheets in the metadata and get their names
        for sheet_m in sheet_meta:
            # get name and append to list of sheet names
            sheet_name = sheet_m['properties']['title']
            sheet_names.append(sheet_name)

    # Make 2 api calls to get the data for all sheets and ranges
    # Get values where dates are a string and get values where dates are a serial number, this is done so the fields which are dates can be distinguished and properly calculated
    # get values as a string
    values = sheet.values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=sheet_names,
        # unformatted returns typed values
        valueRenderOption="UNFORMATTED_VALUE",
        # will return formatted dates as string
        dateTimeRenderOption="FORMATTED_STRING"
    ).execute()['valueRanges']

    # get dates as serial number
    values_date_sn = sheet.values().batchGet(
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


if __name__ == "__main__":
    # timing purposes
    start = time.time()

    # FULL PIPELINE RUN
    pipeline = dlt.pipeline(destination="postgres", full_refresh=False, dataset_name="sample_google_sheet_data")
    data = google_spreadsheet(spreadsheet_url=SPREADSHEET_URL)
    info = pipeline.run(data)
    print(info)

    # time execution time
    end = time.time()
    print(f'Execution time of the program: {end-start}')
