"""This source collects inbox emails, downloads attachments to local folder and stores all info in destination"""
import email
import imaplib
import os
from copy import deepcopy
from email.message import Message
from typing import Any, Optional, Sequence

import dlt
from dlt.common import logger, pendulum
from dlt.extract.source import DltResource, TDataItem

from .settings import DEFAULT_START_DATE, FILTER_EMAILS, STORAGE_FOLDER_PATH


@dlt.source
def inbox_source(
    storage_folder_path: str = STORAGE_FOLDER_PATH,
    filter_emails: Sequence[str] = FILTER_EMAILS,
    attachments: bool = False,
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
) -> DltResource:
    uids = messages_uids(
        filter_emails=filter_emails, folder="INBOX", start_date=start_date
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
    folder: str = "INBOX",
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
    initial_message_num: Optional[Any] = dlt.sources.incremental(
        "message_uid", initial_value=1
    ),
) -> TDataItem:
    last_message_num = initial_message_num.last_value

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        client.select(folder, readonly=True)
        criteria = [f'(SINCE {start_date.strftime("%d-%b-%Y")})']

        if filter_emails:
            logger.info(f"Load emails only from: {filter_emails}")
            for email_ in filter_emails:
                criteria.extend([f"(FROM {email_})"])

        status, messages = client.uid(
            "search", "UID " + str(int(last_message_num)) + ":*", *criteria
        )
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
) -> TDataItem:
    message_uid = str(item["message_uid"])

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        msg = get_message_obj(client, message_uid)
        if msg:
            email_data = dict(msg)
            email_data["message_uid"] = int(message_uid)
            email_data["date"] = pendulum.parse(msg["Date"], strict=False)
            email_data["content_type"] = msg.get_content_type()
            email_data["body"] = get_email_body(msg)

            yield email_data


@dlt.transformer(name="attachments", write_disposition="replace")
def get_attachments_by_uid(
    item: TDataItem,
    storage_folder_path: str,
    host: str = dlt.secrets.value,
    email_account: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
) -> TDataItem:
    message_uid = str(item["message_uid"])

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        msg = get_message_obj(client, message_uid)
        if msg:
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
                        result.update(
                            {
                                "file_name": filename,
                                "file_path": os.path.abspath(attachment_path),
                                "content_type": part.get_content_type(),
                            }
                        )

                        yield result


def get_message_obj(client: imaplib.IMAP4_SSL, message_uid: str) -> Optional[Message]:
    client.select()

    status, data = client.uid("fetch", message_uid, "(RFC822)")

    msg = None
    if status == "OK":
        raw_email = data[0]
        if raw_email:
            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)

    return msg


def get_email_body(msg: Message) -> str:
    """
    Get the body of the email message.

    Parameters:
        msg (Message): The email message object.

    Returns:
        str: The email body as a string.
    """
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                body += part.get_payload(decode=True).decode(errors="ignore")
    else:
        body = msg.get_payload(decode=True).decode(errors="ignore")

    return body
