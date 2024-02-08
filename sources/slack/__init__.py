"""Fetches Slack Conversations, History and logs."""

from typing import Iterable, List, Optional, Tuple

import dlt
from dlt.common.typing import TAnyDateTime, TDataItem
from dlt.sources import DltResource
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
    replies: bool = False,
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
        replies: Boolean flag indicating if you want a replies table to be present as well. False by default.

    Returns:
        Iterable[DltResource]: A list of DltResource objects representing the data resources.
    """

    end_dt: Optional[DateTime] = ensure_dt_type(end_date)
    start_dt: Optional[DateTime] = ensure_dt_type(start_date)

    api = SlackAPI(
        access_token=access_token,
        page_size=page_size,
    )

    def get_channels(
        slack_api: SlackAPI, selected_channels: Optional[List[str]]
    ) -> Tuple[List[TDataItem], List[TDataItem]]:
        """
        Returns channel fetched from slack and list of selected channels.

        Args:
            slack_api: Slack API instance.
            selected_channels: List of selected channels names or None.

        Returns:
            Tuple[List[TDataItem], List[TDataItem]]: fetched channels and selected fetched channels.
        """
        channels: List[TDataItem] = []
        for page_data in slack_api.get_pages(
            resource="conversations.list",
            response_path="$.channels[*]",
            datetime_fields=DEFAULT_DATETIME_FIELDS,
        ):
            channels.extend(page_data)

        if selected_channels:
            fetch_channels = [
                c
                for c in channels
                if c["name"] in selected_channels or c["id"] in selected_channels
            ]
        else:
            fetch_channels = channels
        return channels, fetch_channels

    channels, fetched_selected_channels = get_channels(api, selected_channels)

    @dlt.resource(name="channels", primary_key="id", write_disposition="replace")
    def channels_resource() -> Iterable[TDataItem]:
        """Yield all channels as a DLT resource."""
        yield from channels

    @dlt.resource(name="users", primary_key="id", write_disposition="replace")
    def users_resource() -> Iterable[TDataItem]:
        """
        Yield all users as a DLT resource.

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

    @dlt.resource(
        name="messages",
        primary_key=("channel", "ts"),
        columns={"blocks": {"data_type": "complex"}},
        write_disposition="append",
    )
    def messages_resource(
        created_at: dlt.sources.incremental[DateTime] = dlt.sources.incremental(
            "ts",
            initial_value=start_dt,
            end_value=end_dt,
            allow_external_schedulers=True,
        ),
    ) -> Iterable[TDataItem]:
        """
        Yield all messages for a set of selected channels as a DLT resource. Keep blocks column without normalization.

        Args:
            created_at (dlt.sources.incremental[DateTime]): The incremental created_at field.

        Yields:
            Iterable[TDataItem]: A list of messages.
        """
        start_date_ts = ensure_dt_type(created_at.last_value, to_ts=True)
        end_date_ts = ensure_dt_type(created_at.end_value, to_ts=True)
        for channel_data in fetched_selected_channels:
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

    # It will not work in the pipeline or tests because it is a paid feature,
    # raise an error when it is not a paying account.
    @dlt.resource(
        name="access_logs",
        selected=False,
        primary_key="user_id",
        write_disposition="append",
    )
    # it is not an incremental resource it just has an end_date filter
    def logs_resource() -> Iterable[TDataItem]:
        """The access logs resource."""
        for page_data in api.get_pages(
            resource="team.accessLogs",
            response_path="$.logins[*]",
            datetime_fields=["date_first", "date_last"],
            params={"before": end_dt if end_dt is None else end_dt.int_timestamp},
        ):
            yield page_data

    @dlt.transformer(
        data_from=messages_resource,
        primary_key=("thread_ts", "ts"),
        name="replies",
        columns={"blocks": {"data_type": "complex"}},
        write_disposition="append",
    )
    @dlt.defer
    def replies_resource(messages: List[TDataItem]) -> Iterable[TDataItem]:
        """
        Yield all the threads replies for selected channels.

        Args:
            message: Dict containing one message data.

        Yields:
            Iterable[TDataItem]: A list of replies.
        """
        for message in messages:
            params = {
                "channel": message["channel"],
                "ts": ensure_dt_type(message["ts"], to_ts=True),
            }

            for page_data in api.get_pages(
                resource="conversations.replies",
                response_path="$.messages[*]",
                params=params,
                datetime_fields=MSG_DATETIME_FIELDS,
                context={"channel": message["channel"]},
            ):
                yield [mes for mes in page_data if "parent_user_id" in mes]

    yield from (channels_resource, users_resource, messages_resource, logs_resource)
    if replies:
        yield replies_resource
