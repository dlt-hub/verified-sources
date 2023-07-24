"""This resource collects attachments from Gmail inboxes to destinations"""
import os
from typing import Any, Optional, Sequence, Union

import dlt
from dlt.common import logger, pendulum
from dlt.extract.source import TDataItem
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials

from .helpers import GmailClient, Message
from .settings import FILTER_EMAILS, HEADERS_DEFAULT, STORAGE_FOLDER_PATH


@dlt.resource(write_disposition="replace")
def gmail(
    credentials: Union[
        GcpOAuthCredentials, GcpServiceAccountCredentials
    ] = dlt.secrets.value,
    storage_folder_path: str = STORAGE_FOLDER_PATH,
    filter_emails: Sequence[str] = FILTER_EMAILS,
    download: bool = False,
    internal_date: Optional[Any] = dlt.sources.incremental(
        "internal_date", initial_value=0
    ),
) -> TDataItem:
    last_date_string = pendulum.from_timestamp(
        internal_date.last_value
    ).to_date_string()
    client = GmailClient(credentials)

    emails = filter_emails or [" "]
    for email in emails:
        yield from pagination(
            client,
            f"is:inbox after:{last_date_string} from:{email}",
            download,
            storage_folder_path,
        )


def pagination(
    client: GmailClient,
    query: str = "is:inbox",
    download: bool = False,
    storage_folder_path: str = STORAGE_FOLDER_PATH,
):
    page_token = None
    while True:
        messages_info = client.messages_info(
            user_id="me", page_token=page_token, query=query
        )
        messages_ids = [message["id"] for message in messages_info.get("messages", [])]

        if not messages_ids:
            logger.warning("No messages found in the inbox.")
        else:
            for message_id in messages_ids:
                message = client.get_one_message(user_id="me", message_id=message_id)
                res = parse_message(message, download, storage_folder_path)
                yield from res

        page_token = messages_info.get("nextPageToken")

        if not page_token:
            break


def parse_message(
    message: Message,
    download: bool = False,
    storage_folder_path: str = STORAGE_FOLDER_PATH,
) -> Optional[TDataItem]:
    content = message.content()

    payload = content.get("payload")

    parts = [payload]
    while parts:
        response = {
            "message_id": message.message_id,
            "internal_date": int(content.get("internalDate")) / 1000,
            "message_body": content.get("snippet"),
            "size_estimate": content.get("sizeEstimate"),
            "file_path": None,
        }
        part = parts.pop()
        if part.get("parts"):
            parts.extend(part["parts"])

        headers = part.get("headers")

        for header in headers:
            name = header.get("name")
            if name and name in HEADERS_DEFAULT:
                response[name] = header.get("value")
                if name == "Date":
                    response[name] = pendulum.parse(header.get("value"), strict=False)

        body = part.get("body")

        if part.get("filename"):
            if "data" in body:
                file_data = message.encode_attachment_data(body["data"])
            elif "attachmentId" in body:
                response["attachment_id"] = body["attachmentId"]
                attachment = message.attachments(attachment_id=body["attachmentId"])
                file_data = message.encode_attachment_data(attachment["data"])
            else:
                file_data = None

            if file_data is not None:
                response["file_name"] = part["filename"]

                if download:
                    save_path = os.path.join(
                        storage_folder_path, message.message_id + "_" + part["filename"]
                    )
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    with open(save_path, "wb") as f:
                        f.write(file_data)

                    response["file_path"] = os.path.abspath(save_path)

        yield response
