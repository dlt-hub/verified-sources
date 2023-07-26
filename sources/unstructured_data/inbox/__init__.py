"""This source collects inbox emails, downloads attachments to local folder and stores all info in destination"""
import os
from typing import Any, Optional, Sequence, Dict
import email
import imaplib


import dlt
from dlt.common import logger, pendulum
from dlt.extract.source import TDataItem, DltResource

from .settings import FILTER_EMAILS, STORAGE_FOLDER_PATH


@dlt.source
def inbox_source(
    storage_folder_path: str = STORAGE_FOLDER_PATH,
    filter_emails: Sequence[str] = FILTER_EMAILS,
    attachments: bool = False,

) -> DltResource:
    if attachments:
        return inbox_messages(filter_emails=filter_emails) | get_attachments_by_uid(storage_folder_path=storage_folder_path)

    else:
        return inbox_messages(filter_emails=filter_emails)


@dlt.resource(write_disposition="replace")
def inbox_messages(
    credentials: Dict[str, str] = dlt.secrets.value,
    filter_emails: Sequence[str] = FILTER_EMAILS,
    folder: str = "INBOX",
    initial_date: Optional[Any] = dlt.sources.incremental(
        "date", initial_value=pendulum.datetime(2000, 1, 1)
    ),
) -> TDataItem:

    last_date = initial_date.last_value.strftime("%d-%b-%Y")

    def read_messages(client_: imaplib.IMAP4_SSL, criteria_: Sequence[Any]):
        _, messages = client_.search(None, *criteria_)
        message_ids = messages[0].split()

        if not message_ids:
            logger.warning("No emails found.")

        for message_id in message_ids:
            _, data = client_.fetch(message_id, "(RFC822)")
            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)
            email_data = {
                'message_uid': message_id,
                'message_id': msg['Message-ID'],
                'from': msg['From'],
                'subject': msg['Subject'],
                'date': pendulum.parse(msg['Date'], strict=False),
                'content_type': msg.get_content_type(),
                'body': get_email_body(msg),
            }
            yield email_data

    with imaplib.IMAP4_SSL(credentials["host"]) as client:
        client.login(credentials["username"], credentials["password"])
        client.select(folder, readonly=True)
        criteria = [f'(SENTSINCE {last_date})']

        if filter_emails:
            logger.info(f"Load emails only from: {filter_emails}")

            for email_ in filter_emails:
                criteria.extend([f'(FROM {email_})'])
                yield from read_messages(client, criteria)
        else:
            yield from read_messages(client, criteria)


@dlt.transformer(name="attachments", write_disposition="replace")
def get_attachments_by_uid(
    item: TDataItem,
    storage_folder_path: str,
    credentials: Dict[str, str] = dlt.secrets.value
) -> TDataItem:
    message_uid = item["message_uid"]
    with imaplib.IMAP4_SSL(credentials["host"]) as client:
        client.login(credentials["username"], credentials["password"])
        client.select()

        response, data = client.uid('fetch', message_uid, '(RFC822)')

        if response == 'OK':
            raw_email = data[0]
            if raw_email:
                raw_email = data[0][1]
                msg = email.message_from_bytes(raw_email)

                for part in msg.walk():
                    content_disposition = part.get("Content-Disposition", "")
                    if "attachment" in content_disposition:
                        filename = part.get_filename()
                        if filename:
                            attachment_data = part.get_payload(decode=True)
                            attachment_path = os.path.join(storage_folder_path, message_uid.decode("utf-8")  + filename)
                            os.makedirs(os.path.dirname(attachment_path), exist_ok=True)

                            with open(attachment_path, 'wb') as f:
                                f.write(attachment_data)

                            item.update({"file_name": filename, "file_path": os.path.abspath(attachment_path)})

                            yield item


def get_email_body(msg: email.message.Message) -> str:
    """
    Get the body of the email message.

    Parameters:
        msg (email.message.Message): The email message object.

    Returns:
        str: The email body as a string.
    """
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                body += part.get_payload(decode=True).decode(errors='ignore')
    else:
        body = msg.get_payload(decode=True).decode(errors='ignore')

    return body