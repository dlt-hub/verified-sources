from typing import Iterator, Dict
import re

import dlt
from dlt.common.typing import TDataItems
from dlt.common.configuration.specs import configspec, BaseConfiguration
from dlt.common import logger
import pandas as pd

from .helpers import SharepointClient
from .sharepoint_files_config import SharepointFilesConfig, SharepointListConfig


@configspec
class SharepointCredentials(BaseConfiguration):
    client_id: str = None
    tenant_id: str = None
    site_id: str = None
    client_secret: str = None
    sub_site_id: str = ""


@dlt.source(name="sharepoint_list", max_table_nesting=0)
def sharepoint_list(
    sharepoint_list_config: SharepointListConfig,
    credentials: SharepointCredentials = dlt.secrets.value,
) -> Iterator[Dict[str, str]]:
    client: SharepointClient = SharepointClient(**credentials)
    client.connect()
    logger.info(f"Connected to SharePoint site: {client.site_info}")

    def get_pipe(sharepoint_list_config: SharepointListConfig):
        def get_records(sharepoint_list_config: SharepointListConfig):
            data = client.get_items_from_list(list_title=sharepoint_list_config.list_title, select=sharepoint_list_config.select)
            yield from data
        return dlt.resource(get_records, name=sharepoint_list_config.table_name)(sharepoint_list_config)
    yield get_pipe(sharepoint_list_config=sharepoint_list_config)


@dlt.source(name="sharepoint_files", max_table_nesting=0)
def sharepoint_files(
    sharepoint_files_config: SharepointFilesConfig,
    credentials: SharepointCredentials = dlt.secrets.value,
):
    client: SharepointClient = SharepointClient(**credentials)
    client.connect()
    logger.info(f"Connected to SharePoint site: {client.site_info}")

    def get_files(
        sharepoint_files_config: SharepointFilesConfig,
        last_update_timestamp: dlt.sources.incremental = dlt.sources.incremental(
            cursor_path="lastModifiedDateTime",
            initial_value="2020-01-01T00:00:00Z",
            primary_key=(),
        ),
    ):
        current_last_value = last_update_timestamp.last_value
        logger.debug(f"current_last_value: {current_last_value}")
        for file_item in client.get_files_from_path(
            folder_path=sharepoint_files_config.folder_path,
            file_name_startswith=sharepoint_files_config.file_name_startswith,
            pattern=sharepoint_files_config.pattern,
        ):
            logger.debug(
                "filtering files based on lastModifiedDateTime, compare to last_value:"
                f" {current_last_value}"
            )
            if file_item["lastModifiedDateTime"] > current_last_value or not sharepoint_files_config.is_file_incremental:
                logger.info(
                    f"Processing file after lastModifiedDateTime filter: {file_item['name']}"
                )

                file_item["pd_function"] = sharepoint_files_config.file_type.get_pd_function()
                file_item["pd_kwargs"] = sharepoint_files_config.pandas_kwargs
                yield file_item
            else:
                logger.info(
                    f"Skipping file {file_item['name']} based on lastModifiedDateTime filter"
                )

    def get_records(file_item: Dict) -> TDataItems:
        chunksize = file_item["pd_kwargs"].get("chunksize", None)
        file_io = client.get_file_bytes_io(file_item=file_item)

        if chunksize:
            with file_item["pd_function"](file_io, **file_item["pd_kwargs"]) as reader:
                for num, chunk in enumerate(reader):
                    logger.info(f"Processing chunk {num} of {file_item['name']}")
                    yield chunk
        else:
            df = file_item["pd_function"](file_io, **file_item["pd_kwargs"])
            yield df
        logger.debug(f"get_records done for {file_item['name']}")

    def get_pipe(sharepoint_files_config: SharepointFilesConfig):
        return dlt.resource(get_files, name=f"{sharepoint_files_config.table_name}_files")(sharepoint_files_config) | dlt.transformer(
            get_records, name=sharepoint_files_config.table_name, parallelized=False
        )

    yield get_pipe(sharepoint_files_config=sharepoint_files_config)
