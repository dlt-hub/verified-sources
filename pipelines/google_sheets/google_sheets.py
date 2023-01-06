from typing import Any, Iterator, Sequence, Union, cast

import dlt
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItems

try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google Sheets Source", ["google1", "google2"])


# gets sheet values from a single spreadsheet preserving original typing for schema generation
# TODO: write schema discovery to better handle dates
# TODO: consider using https://github.com/burnash/gspread for spreadsheet discovery
# TODO: add exceptions to the code in case HTTP requests fail

# this function receives a sheet name, a working authentication
def get_sheet(sheet_name: str, sheets: Resource, spreadsheet_id: str) -> Iterator[DictStrAny]:

    value = sheets.values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        # range=range_value,
        # unformatted returns typed values
        valueRenderOption="UNFORMATTED_VALUE",
        # will return formatted dates
        dateTimeRenderOption="FORMATTED_STRING"
    ).execute()
    values = value.get('values')

    # yield dicts assuming row 0 contains headers and following rows values and all rows have identical length
    for v in values[1:]:
        yield {h: v for h, v in zip(values[0], v)}


def _initialize_sheets(credentials: GcpClientCredentialsWithDefault) -> Any:
    # Build the service object for Google sheets api.
    service = build('sheets', 'v4', credentials=credentials.to_service_account_credentials())
    return service


@dlt.source
def google_spreadsheet(spreadsheet_id: str, sheet_names: list[str] = None, credentials: GcpClientCredentialsWithDefault = dlt.secrets.value) -> Any:

    # authenticate to the service using the helper function
    service = _initialize_sheets(cast(GcpClientCredentialsWithDefault, credentials))
    sheet = service.spreadsheets()

    # if sheet names is left empty
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

    # execute the get sheet function to create a resource table with the name of every sheet in the request
    # create resources from supplied sheet names
    return [dlt.resource(get_sheet(sheet_name=name, sheets=sheet, spreadsheet_id=spreadsheet_id), name=name, write_disposition="replace") for name in sheet_names]


@dlt.source
def api_test(spreadsheet_id: str, credentials: GcpClientCredentialsWithDefault = dlt.secrets.value) -> Any:
    service = _initialize_sheets(cast(GcpClientCredentialsWithDefault, credentials))
    # get list of  typed values
    sheet = service.spreadsheets()

    # get metadata of spreadsheet to check for number of sheets inside
    metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
    sheet_meta = metadata['sheets']
    for sheet_m in sheet_meta:
        sheet_name = sheet_m['properties']['title']
        print(f' Processing sheet: {sheet_name}')

        # get values for every sheet
        value = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name,
            # range=range_value,
            # unformatted returns typed values
            valueRenderOption="UNFORMATTED_VALUE",
            # will return formatted dates
            dateTimeRenderOption="FORMATTED_STRING"
        ).execute()
        row_val = value.get('values')
        print(row_val)
        print("-" * 35)

        # print the sheets assuming row 0 contains headers and following rows values and all rows have identical length
        header = row_val[0]      # list
        print(f'Header: {header}')
        for row in row_val[1:]:
            print(f'Row: {row}')

    # configured for testing purposes, does nothing
    @dlt.resource(write_disposition="replace")
    def test() -> Iterator[TDataItems]:
        for test_val in range(10):
            yield test_val
    return test()
