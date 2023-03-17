"""Loads Google Sheets data from tabs, named and explicit ranges. Contains the main pipeline functions """

from typing import Iterator, List

import dlt
from dlt.common import logger
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.typing import DictStrAny, Dict, TDataItem, StrAny
from dlt.common.exceptions import MissingDependencyException
from dlt.extract.source import DltResource
from .helpers.data_processing import convert_named_range_to_a1, get_spreadsheet_id, process_range
from .helpers.api_calls import api_auth
from .helpers import api_calls

try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google API Client", ["google-api-python-client"])


@dlt.source
def google_spreadsheet(spreadsheet_identifier: str = dlt.config.value, range_names: List[str] = dlt.config.value, credentials: GcpClientCredentialsWithDefault = dlt.secrets.value,
                       get_sheets: bool = True, get_named_ranges: bool = True) -> List[DltResource]:
    """
    The source for the dlt pipeline. It returns the following resources: 1 dlt resource for every range in sheet_names
    @:param: spreadsheet_identifier - the id or url to the spreadsheet
    @:param: sheet_names - A list of ranges in the spreadsheet of the type sheet_name!range_name. These are the ranges to be converted into tables
    @:param: credentials - GCP credentials to the account with Google Sheets api access, defined in dlt.secrets
    @:return data_resources_list: list of dlt resources
    """
    # authenticate to the service using the helper function
    service = api_auth(credentials)
    logger.info("Successful Authentication")
    # get spreadsheet id from url or id
    spreadsheet_id = get_spreadsheet_id(spreadsheet_identifier)
    # if range_names were not provided, initialize them as an empty list
    if not range_names:
        range_names = []
    named_ranges = None
    # if sheet names or named_ranges are to be added as tables, an extra api call is made and
    if get_sheets or get_named_ranges:
        # get metadata and append to list of ranges as needed
        simple_metadata = api_calls.get_metadata_simple(spreadsheet_id=spreadsheet_id, service=service)
        if get_sheets:
            range_names += list(simple_metadata["sheets"].values())
        if get_named_ranges:
            named_ranges = {convert_named_range_to_a1(named_range_dict=named_range, sheet_names_dict=simple_metadata["sheets"]): named_range["name"] for named_range in simple_metadata["named_ranges"]}
            range_names += list(named_ranges.keys())
    # get data and metadata
    metadata_ranges_all = api_calls.get_metadata(spreadsheet_id=spreadsheet_id, service=service, ranges=range_names, named_ranges=named_ranges)
    data_resources_list: List[DltResource] = get_data(service=service, spreadsheet_id=spreadsheet_id, range_names=range_names, metadata_dict=metadata_ranges_all)

    # create metadata resource
    metadata_resource = metadata_table(spreadsheet_info=metadata_ranges_all, spreadsheet_id=spreadsheet_id)
    data_resources_list.append(metadata_resource)
    return data_resources_list


@dlt.resource(write_disposition="replace", name="spreadsheet_info")
def metadata_table(spreadsheet_info: StrAny, spreadsheet_id: str) -> Iterator[TDataItem]:
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
        range_sheet_name = loaded_range_meta["range"].split("!")[0]
        # table structure
        table_dict = {
            "spreadsheet_id": spreadsheet_id,
            "loaded_range": loaded_range,
            "sheet_name": range_sheet_name,
            "num_cols": range_num_headers
        }
        # change name of loaded range name if it is a ranged name
        if loaded_range_meta["name"]:
            table_dict["loaded_range"] = loaded_range_meta["name"]
        yield table_dict


def get_data(service: Resource, spreadsheet_id: str, range_names: List[str], metadata_dict: Dict[str, DictStrAny]) -> List[DltResource]:
    """
    Makes an api call to Google sheets and retrieve all the ranges listed in range_names and process them into dlt resources
    @:param: service - Object to make api calls to Google Sheets
    @:param: spredsheet_id - the id of the spreadsheet
    @:param: range_names - list of range names
    @:param: metadata_dict - the dict with metadata
    @:returns: my_resources - list of dlt resources, each containing a table of a specific range
    """

    # get data from Google Sheets and iterate through them to process each range into a separate dlt resource
    my_resources = []
    values = api_calls.get_data_batch(service=service, spreadsheet_id=spreadsheet_id, range_names=range_names)
    for i in range(len(values)):
        # get range name and metadata for sheet
        sheet_range_name = values[i]["range"]
        if sheet_range_name in metadata_dict:
            sheet_meta_batch = metadata_dict[sheet_range_name]
            # check if this is a named range and change the name so the range table can be saved with its proper name
            if sheet_meta_batch["name"]:
                sheet_range_name = sheet_meta_batch["name"]
        else:
            # if range doesn't exist as a key for metadata then it means the key is just the sheet name and google sheets api response just filled the range or that the sheet is skipped because
            # it was empty
            sheet_range_name = sheet_range_name.split("!")[0]
            try:
                sheet_meta_batch = metadata_dict[sheet_range_name]
            except KeyError:
                # sheet is not there because it was popped from metadata due to being empty
                logger.warning(f"Skipping data for empty range: {sheet_range_name}")
                continue
        # get range values
        sheet_range = values[i]["values"]
        # create a resource from processing both sheet/range values
        my_resources.append(dlt.resource(process_range(sheet_val=sheet_range, sheet_meta=sheet_meta_batch),
                                         name=sheet_range_name,
                                         write_disposition="replace")
                            )
        logger.info(f"Data for {sheet_range_name} loaded")
    logger.info("Finished loading all ranges")
    return my_resources
