"""This resource collects attachments from Gmail inboxes to destinations"""
import os
from typing import Optional, Sequence, Union

import dlt
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
    fiter_emails: Sequence[str] = FILTER_EMAILS,
    download: bool = False,
) -> TDataItem:
    client = GmailClient(credentials)

    while True:
        messages_info = client.messages_info(user_id="me", message_type="inbox")
        messages_ids = [message["id"] for message in messages_info.get("messages", [])]

        if not messages_ids:
            print("No messages found in the inbox.")
        else:
            for message_id in messages_ids:
                message = client.get_one_message(user_id="me", message_id=message_id)
                yield parse_message(
                    message, download, storage_folder_path, fiter_emails
                )

        if not messages_info.get("nextPageToken"):
            break


def parse_message(
    message: Message,
    download: bool = False,
    storage_folder_path: str = STORAGE_FOLDER_PATH,
    fiter_emails: Sequence[str] = FILTER_EMAILS,
) -> Optional[TDataItem]:
    content = message.content()

    response = {
        "message_id": message.message_id,
        "internal_date": content.get("internalDate"),
        "message_body": content.get("snippet"),
        "size_estimate": content.get("sizeEstimate"),
        "file_path": None,
    }

    payload = content.get("payload")

    parts = [payload]
    while parts:
        part = parts.pop()
        if part.get("parts"):
            parts.extend(part["parts"])

        headers = part.get("headers")

        for header in headers:
            name = header.get("name")
            if name and name in HEADERS_DEFAULT:
                response[name] = header.get("value")

        if fiter_emails and "From" in response:
            if not any(email in response["From"] for email in fiter_emails):
                return None

        body = part.get("body")

        if part.get("filename"):
            response["file_name"] = part["filename"]
            if "data" in body:
                file_data = message.encode_attachment_data(body["data"])
            elif "attachmentId" in body:
                response["attachment_id"] = body["attachmentId"]
                attachment = message.attachments(attachment_id=body["attachmentId"])
                file_data = message.encode_attachment_data(attachment["data"])
            else:
                file_data = None

            if file_data and download:
                save_path = os.path.join(storage_folder_path, message.message_id + "_" + part["filename"])
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, "wb") as f:
                    f.write(file_data)

                response["file_path"] = os.path.abspath(save_path)

        yield response
