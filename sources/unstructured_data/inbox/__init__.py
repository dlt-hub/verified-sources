"""This source collects inbox emails and downloads attachments to local folder"""
import imaplib
import os
from copy import deepcopy
from typing import Optional, Sequence

import dlt
from dlt.common import logger, pendulum
from dlt.extract.source import DltResource, TDataItem

from .helpers import extract_email_info, get_internal_data, get_message_obj
from .settings import (
    DEFAULT_START_DATE,
    FILTER_EMAILS,
    GMAIL_GROUP,
    STORAGE_FOLDER_PATH,
)


@dlt.source
def inbox_source(
    storage_folder_path: str = STORAGE_FOLDER_PATH,
    filter_emails: Sequence[str] = FILTER_EMAILS,
    gmail_group: Optional[str] = GMAIL_GROUP,
    attachments: bool = False,
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
) -> DltResource:
    uids = messages_uids(
        filter_emails=filter_emails,
        gmail_group=gmail_group,
        folder="INBOX",
        start_date=start_date,
    )

    if attachments:
        return uids | get_attachments_by_uid(storage_folder_path=storage_folder_path)
    else:
        return uids | read_messages


@dlt.resource
def messages_uids(
    host: str = dlt.secrets.value,
    email_account: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    filter_emails: Sequence[str] = FILTER_EMAILS,
    gmail_group: Optional[str] = GMAIL_GROUP,
    folder: str = "INBOX",
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
    initial_message_num: Optional[
        dlt.sources.incremental[int]
    ] = dlt.sources.incremental("message_uid", initial_value=1),
) -> TDataItem:
    last_message_num = initial_message_num.last_value

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        client.select(folder, readonly=True)
        criteria = [
            f"(SINCE {start_date.strftime('%d-%b-%Y')})",
            f"(UID {str(int(last_message_num))}:*)",
        ]

        if filter_emails:
            logger.info(f"Load emails only from: {filter_emails}")
            for email_ in filter_emails:
                criteria.extend([f"(FROM {email_})"])

        if gmail_group:
            logger.info(f"Load all emails for Group: {gmail_group}")
            criteria.extend([f"(TO {gmail_group})"])

        status, messages = client.uid("search", *criteria)
        message_uids = messages[0].split()

        if not message_uids:
            logger.warning("No emails found.")
        else:
            yield from [
                {"message_uid": int(message_uid)} for message_uid in message_uids
            ]


@dlt.transformer(name="messages", write_disposition="replace")
def read_messages(
    item: TDataItem,
    host: str = dlt.secrets.value,
    email_account: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    body: bool = False,
) -> TDataItem:
    message_uid = str(item["message_uid"])

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        msg = get_message_obj(client, message_uid)
        if msg:
            result = deepcopy(item)
            result.update(extract_email_info(msg, body=body))
            result.update({"internal_date": get_internal_data(client, message_uid)})
            yield result


@dlt.transformer(name="attachments", write_disposition="replace")
def get_attachments_by_uid(
    item: TDataItem,
    storage_folder_path: str,
    host: str = dlt.secrets.value,
    email_account: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    body: bool = False,
) -> TDataItem:
    message_uid = str(item["message_uid"])

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        msg = get_message_obj(client, message_uid)
        if msg:
            email_info = extract_email_info(msg, body=body)
            internal_date = get_internal_data(client, message_uid)

            for part in msg.walk():
                content_disposition = part.get("Content-Disposition", "")
                if "attachment" in content_disposition:
                    filename = part.get_filename()
                    if filename:
                        attachment_data = part.get_payload(decode=True)
                        attachment_path = os.path.join(
                            storage_folder_path, message_uid + filename
                        )
                        os.makedirs(os.path.dirname(attachment_path), exist_ok=True)

                        with open(attachment_path, "wb") as f:
                            f.write(attachment_data)

                        result = deepcopy(item)
                        result.update(email_info)
                        result.update(
                            {
                                "file_name": filename,
                                "file_path": os.path.abspath(attachment_path),
                                "content_type": part.get_content_type(),
                                "internal_date": internal_date,
                            }
                        )

                        yield result
