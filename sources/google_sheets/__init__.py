"""Loads Google Sheets data from tabs, named and explicit ranges. Contains the main source functions."""

from typing import List, Sequence, Union, Iterable

import dlt
from dlt.sources.credentials import GcpServiceAccountCredentials, GcpOAuthCredentials
from dlt.extract.source import DltResource

from .helpers.data_processing import (
    get_data_types,
    get_range_headers,
    get_spreadsheet_id,
    process_range,
)
from .helpers.api_calls import api_auth
from .helpers import api_calls


@dlt.source
def google_spreadsheet(
    spreadsheet_identifier: str = dlt.config.value,
    range_names: Sequence[str] = dlt.config.value,
    credentials: Union[
        GcpOAuthCredentials, GcpServiceAccountCredentials
    ] = dlt.secrets.value,
    get_sheets: bool = True,
    get_named_ranges: bool = True,
) -> Iterable[DltResource]:
    """
    The source for the dlt pipeline. It returns the following resources:
    - 1 dlt resource for every range in range_names.
    - Optionally, dlt resources for all sheets inside the spreadsheet and all named ranges inside the spreadsheet.

    Args:
        spreadsheet_identifier (str): The ID or URL of the spreadsheet.
        range_names (Sequence[str]): A list of ranges in the spreadsheet of the format "sheet_name!range_name".
            These are the ranges to be converted into tables.
        credentials (Union[GcpServiceAccountCredentials, GcpOAuthCredentials]): GCP credentials to the account
            with Google Sheets API access, defined in dlt.secrets.
        get_sheets (bool, optional): If True, load all the sheets inside the spreadsheet into the database.
            Defaults to True.
        get_named_ranges (bool, optional): If True, load all the named ranges inside the spreadsheet into the database.
            Defaults to True.

    Yields:
        Iterable[DltResource]: List of dlt resources.
    """
    # authenticate to the service using the helper function
    service = api_auth(credentials)
    # get spreadsheet id from url or id
    spreadsheet_id = get_spreadsheet_id(spreadsheet_identifier)
    all_range_names = set(range_names or [])
    # if no explicit ranges, get sheets and named ranges from metadata
    if not range_names:
        # get metadata with list of sheets and named ranges in the spreadsheet
        sheet_names, named_ranges = api_calls.get_known_range_names(
            spreadsheet_id=spreadsheet_id, service=service
        )
        if get_sheets:
            all_range_names.update(sheet_names)
        if get_named_ranges:
            all_range_names.update(named_ranges)

    # TODO: raise on empty list of ranges with a user friendly exception

    print(all_range_names)

    # first we get all data for all the ranges (explicit or named)
    range_values = api_calls.get_data_for_ranges(
        service=service,
        spreadsheet_id=spreadsheet_id,
        range_names=list(all_range_names),
    )
    assert len(all_range_names) == len(
        range_values
    ), "Google Sheets API must return values for all requested ranges"

    # get metadata for two first rows of each range
    # first row MUST contain headers
    # second row contains data which we'll use to sample data types.
    # google sheets return datetime and date types as lotus notes serial number. which is just a float so we cannot infer the correct types just from the data
    meta_ranges: List[str] = []
    for name, range_value in zip(all_range_names, range_values):
        parsed_range = range_value[0]
        # create a new range to get first two rows
        meta_range = parsed_range._replace(end_row=parsed_range.start_row + 1)
        print(f"{name}:{parsed_range}:{meta_range}")
        meta_ranges.append(str(meta_range))

    # print(meta_values)
    # with open("meta_values_2.json", "wb") as f:
    #     json.dump(meta_values, f, pretty=True)

    metadata_table = []
    meta_values = (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, ranges=meta_ranges, includeGridData=True)
        .execute()
    )
    for name, range_value in zip(all_range_names, range_values):
        parsed_range = range_value[0]
        # here is a tricky part due to how Google Sheets API returns the metadata. We are not able to directly pair the input range names with returned metadata objects
        # instead metadata objects are grouped by sheet names, still each group order preserves the order of input ranges
        # so for each range we get a sheet name, we look for the metadata group for that sheet and then we consume first object on that list with pop
        print(
            next(
                sheet
                for sheet in meta_values["sheets"]
                if sheet["properties"]["title"] == parsed_range.sheet_name
            )
        )
        metadata = next(
            sheet
            for sheet in meta_values["sheets"]
            if sheet["properties"]["title"] == parsed_range.sheet_name
        )["data"].pop(0)

        if "rowData" not in metadata:
            # metadata may be empty, in that case there's no data at all to be returned so we skip this
            pass
            # print(f"skipping {name}")
        else:
            headers = get_range_headers(metadata["rowData"], name)
            data_types = get_data_types(metadata["rowData"])

            yield dlt.resource(
                process_range(
                    range_value[1][1:], headers=headers, data_types=data_types
                ),
                name=name,
                write_disposition="replace",
            )
        metadata_table.append(
            {
                "spreadsheet_id": spreadsheet_id,
                "range_name": name,
                "range": str(parsed_range),
                "range_parsed": parsed_range._asdict(),
            }
        )
    yield dlt.resource(
        metadata_table, write_disposition="replace", name="spreadsheet_info"
    )
