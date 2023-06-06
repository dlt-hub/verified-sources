"""Loads Google Sheets data from tabs, named and explicit ranges. Contains the main source functions."""

from typing import Iterator, List, Sequence, Union, Iterable

import dlt
from dlt.common import logger
from dlt.common.typing import DictStrAny, Dict, TDataItem, StrAny
from dlt.common.exceptions import MissingDependencyException
from dlt.sources.credentials import GcpServiceAccountCredentials, GcpOAuthCredentials
from dlt.extract.source import DltResource

from .helpers.data_processing import (
    convert_named_range_to_a1,
    get_spreadsheet_id,
    process_range,
)
from .helpers.api_calls import api_auth
from .helpers import api_calls

from apiclient.discovery import Resource


@dlt.source
def google_spreadsheet(
    spreadsheet_identifier: str = dlt.config.value,
    range_names: Sequence[str] = dlt.config.value,
    credentials: Union[
        GcpServiceAccountCredentials, GcpOAuthCredentials
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
    # Initialize a list with the values in range_names (can be an array if declared in config.toml). This needs to be converted to a list because it will be used as input by google-api-python-client
    # type needs to be checked instead of isinstance since toml Arrays have lists as a superclass
    if type(range_names) is not list:
        ranges_list = [range_name for range_name in range_names]
    else:
        ranges_list = range_names
    # if sheet names or named_ranges are to be added as tables, an extra api call is made.
    named_ranges = None
    if get_sheets or get_named_ranges:
        # get metadata and append to list of ranges as needed
        simple_metadata = api_calls.get_metadata_simple(
            spreadsheet_id=spreadsheet_id, service=service
        )
        if get_sheets:
            ranges_list += list(simple_metadata["sheets"].values())
        if get_named_ranges:
            named_ranges = {
                convert_named_range_to_a1(
                    named_range_dict=named_range,
                    sheet_names_dict=simple_metadata["sheets"],
                ): named_range["name"]
                for named_range in simple_metadata["named_ranges"]
            }
            ranges_list += list(named_ranges.keys())
    # get data and metadata
    metadata_ranges_all = api_calls.get_metadata(
        spreadsheet_id=spreadsheet_id,
        service=service,
        ranges=ranges_list,
        named_ranges=named_ranges,
    )

    # create a list of dlt resources from the data and metadata
    yield from get_data(
        service=service,
        spreadsheet_id=spreadsheet_id,
        range_names=ranges_list,
        metadata_dict=metadata_ranges_all,
    )

    # create metadata resource
    yield metadata_table(
        spreadsheet_info=metadata_ranges_all, spreadsheet_id=spreadsheet_id
    )


@dlt.resource(write_disposition="replace", name="spreadsheet_info")
def metadata_table(
    spreadsheet_info: StrAny, spreadsheet_id: str
) -> Iterator[TDataItem]:
    """
    Creates the metadata_table resource. It adds a table with all loaded ranges into a table.

    Args:
        spreadsheet_info (StrAny): This is a dict where all loaded ranges are keys.
            Inside the dict there is another dict with keys: "headers", "sheet_name", "index" and "values".
        spreadsheet_id (str): The ID of the spreadsheet is included for extra info.

    Yields:
        Iterator[TDataItem]: Generator of dicts with info on ranges that were loaded into the database.
    """

    # get keys for metadata dict and iterate through them
    # the keys for this dict are the ranges where the data is gathered from
    loaded_ranges = spreadsheet_info.keys()
    for loaded_range in loaded_ranges:
        # get needed info from dict
        loaded_range_meta = spreadsheet_info[loaded_range]
        range_num_headers = len(loaded_range_meta["headers"])
        range_sheet_name = loaded_range_meta["range"].split("!")[0]
        # table structure
        table_dict = {
            "spreadsheet_id": spreadsheet_id,
            "loaded_range": loaded_range,
            "sheet_name": range_sheet_name,
            "num_cols": range_num_headers,
        }
        # change name of loaded range name if it is a ranged name
        if loaded_range_meta["name"]:
            table_dict["loaded_range"] = loaded_range_meta["name"]
        yield table_dict


def get_data(
    service: Resource,
    spreadsheet_id: str,
    range_names: List[str],
    metadata_dict: Dict[str, DictStrAny],
) -> Iterable[DltResource]:
    """
    Makes an API call to Google Sheets and retrieves all the ranges listed in range_names.
    Processes them into dlt resources.

    Args:
        service (Resource): Object to make API calls to Google Sheets.
        spreadsheet_id (str): The ID of the spreadsheet.
        range_names (List[str]): List of range names.
        metadata_dict (Dict[str, DictStrAny]): The dictionary with metadata.

    Yields:
        Iterable[DltResource]: List of dlt resources, each containing a table of a specific range.
    """

    # get data from Google Sheets and iterate through them to process each range into a separate dlt resource
    values = api_calls.get_data_batch(
        service=service, spreadsheet_id=spreadsheet_id, range_names=range_names
    )
    for value in values:
        # get range name and metadata for sheet. Extra quotation marks returned by the API call are removed.
        range_part1, range_part2 = value["range"].split("!")
        range_part1 = range_part1.strip("'")
        sheet_range_name = f"{range_part1}!{range_part2}"
        named_range_name = None
        try:
            sheet_meta_batch = metadata_dict[sheet_range_name]
            named_range_name = sheet_meta_batch["name"]
        except KeyError:
            try:
                sheet_range_name = sheet_range_name.split("!")[0]
                sheet_meta_batch = metadata_dict[sheet_range_name]
            except KeyError:
                logger.warning(f"Skipping data for empty range: {sheet_range_name}")
                continue
        # get range values
        sheet_range = value["values"]
        # create a resource from processing both sheet/range values
        yield dlt.resource(
            process_range(sheet_val=sheet_range, sheet_meta=sheet_meta_batch),
            name=named_range_name or sheet_range_name,
            write_disposition="replace",
        )
