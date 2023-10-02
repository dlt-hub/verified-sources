"""Fetches Slack Conversations, History and logs."""

from functools import partial
from typing import Any, Dict, Iterable, List, Optional

import dlt
from dlt.common.typing import TAnyDateTime, TDataItem
from dlt.extract.source import DltResource
from pendulum import DateTime

from .helpers import SlackAPI, ensure_dt_type
from .settings import (
    DEFAULT_DATETIME_FIELDS,
    DEFAULT_START_DATE,
    MAX_PAGE_SIZE,
    MSG_DATETIME_FIELDS,
)


@dlt.source(name="slack", max_table_nesting=2)
def slack_source(
    page_size: int = MAX_PAGE_SIZE,
    access_token: str = dlt.secrets.value,
    start_date: Optional[TAnyDateTime] = DEFAULT_START_DATE,
    end_date: Optional[TAnyDateTime] = None,
    selected_channels: Optional[List[str]] = dlt.config.value,
) -> Iterable[DltResource]:
    """
    The source for the Slack pipeline. Available resources are conversations, conversations_history
    and access_logs.

    Args:
        page_size: The max number of items to fetch per page. Defaults to 1000.
        access_token: the oauth access_token used to authenticate.
        start_date: The start time of the range for which to load. Defaults to January 1st 2000.
        end_date: The end time of the range for which to load data.
        selected_channels: The list of channels to load. If None, all channels will be loaded.

    Returns:
        Iterable[DltResource]: A list of DltResource objects representing the data resources.
    """

    end_dt: Optional[DateTime] = ensure_dt_type(end_date)
    start_dt: Optional[DateTime] = ensure_dt_type(start_date)

    api = SlackAPI(
        access_token=access_token,
        page_size=page_size,
    )

    channels: List[TDataItem] = []
    for page_data in api.get_pages(
        resource="conversations.list",
        response_path="$.channels[*]",
        datetime_fields=DEFAULT_DATETIME_FIELDS,
    ):
        channels.extend(page_data)

    @dlt.resource(name="channels", primary_key="id", write_disposition="replace")
    def channels_resource() -> Iterable[TDataItem]:
        """Yield all channels as a DLT resource."""
        yield from channels

    yield channels_resource

    @dlt.resource(name="users", primary_key="id", write_disposition="replace")
    def users_resource() -> Iterable[TDataItem]:
        """Yield all channels as a DLT resource.

        Yields:
            Iterable[TDataItem]: A list of users.
        """

        for page_data in api.get_pages(
            resource="users.list",
            response_path="$.members[*]",
            params=dict(include_locale=True),
            datetime_fields=DEFAULT_DATETIME_FIELDS,
        ):
            yield page_data

    yield users_resource

    def get_messages_resource(
        channel_data: Dict[str, Any],
        created_at: dlt.sources.incremental[DateTime] = dlt.sources.incremental(
            "ts",
            initial_value=start_dt,
            end_value=end_dt,
            allow_external_schedulers=True,
        ),
    ) -> Iterable[TDataItem]:
        """Yield all messages for a given channel as a DLT resource.

        Args:
            channel_data (Dict[str, Any]): The channel data.
            created_at (dlt.sources.incremental[DateTime]): The incremental created_at field.

        Yields:
            Iterable[TDataItem]: A list of messages.
        """
        start_date_ts = ensure_dt_type(created_at.last_value, to_ts=True)
        end_date_ts = ensure_dt_type(created_at.end_value, to_ts=True)
        params = {
            "channel": channel_data["id"],
            "oldest": start_date_ts,
            "latest": end_date_ts,
        }

        for page_data in api.get_pages(
            resource="conversations.history",
            response_path="$.messages[*]",
            params=params,
            datetime_fields=MSG_DATETIME_FIELDS,
            context={"channel": channel_data["id"]},
        ):
            yield page_data

    if selected_channels:
        fetch_channels = [
            c
            for c in channels
            if c["name"] in selected_channels or c["id"] in selected_channels
        ]
    else:
        fetch_channels = channels

    def table_name_func(channel_name: str, payload: TDataItem) -> str:
        """Return the table name for a given channel and payload."""
        table_type = payload.get("subtype", payload["type"])
        return f"{channel_name}_{table_type}"

    # for each channel, create a messages resource
    for channel in fetch_channels:
        channel_name = channel["name"]
        table_name = partial(table_name_func, channel_name)
        yield dlt.resource(
            get_messages_resource,
            name=channel_name,
            table_name=table_name,
            primary_key=("channel", "ts"),
            write_disposition="append",
        )(channel)

    # It will not work in the pipeline or tests because it is a paid feature, it raises a
    # raise an error when it is not a paying account.
    @dlt.resource(
        name="access_logs",
        selected=False,
        primary_key="user_id",
        write_disposition="append",
    )
    # it is not an incremental resource it just has a end_date filter
    def logs_resource() -> Iterable[TDataItem]:
        """The access logs resource."""
        for page_data in api.get_pages(
            resource="team.accessLogs",
            response_path="$.logins[*]",
            datetime_fields=["date_first", "date_last"],
            params={"before": end_dt if end_dt is None else end_dt.int_timestamp},
        ):
            yield page_data

    yield logs_resource
