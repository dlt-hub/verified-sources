"""This source collects inbox emails and downloads attachments to local folder"""
import imaplib
import os
from copy import deepcopy
from typing import Optional, Sequence
import hashlib

import dlt
from dlt.common import logger, pendulum
from dlt.extract.source import DltResource, TDataItem, TDataItems

from .helpers import extract_attachments, get_message, extract_email_info, get_message_uids
from .settings import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_START_DATE,
    FILTER_EMAILS,
    GMAIL_GROUP,
    STORAGE_PATH,
)
from ..filesystem_source import FilesystemSource


@dlt.source
def inbox_source(
    storage_path: str = STORAGE_PATH,
    gmail_group: Optional[str] = GMAIL_GROUP,
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
    filter_by_emails: Sequence[str] = FILTER_EMAILS,
    filter_by_mime_type: Sequence[str] = (),
    chucksize: int = DEFAULT_CHUNK_SIZE,
) -> DltResource:
    """This source collects inbox emails and downloads attachments to the local folder.

    Args:
        storage_path (str, optional): The local folder path where attachments will be downloaded. Default is 'STORAGE_FOLDER_PATH' from settings.
        gmail_group (str, optional): The email address of the Google Group to filter emails sent to the group. Default is 'GMAIL_GROUP' from settings.
        start_date (pendulum.DateTime, optional): The start date from which to collect emails. Default is 'DEFAULT_START_DATE' from settings.
        filter_by_emails (Sequence[str], optional): A sequence of email addresses used to filter emails based on the 'FROM' field. Default is 'FILTER_EMAILS' from settings.
        filter_by_mime_type (Sequence[str], optional): A sequence of MIME types used to filter attachments based on their content type. Default is an empty sequence.
        chucksize (int, optional): The number of message UIDs to collect at a time. Default is 'DEFAULT_CHUNK_SIZE' from settings.

    Returns:
        DltResource: A dlt resource containing the collected email information.
    """

    uids = messages_uids(
        filter_emails=filter_by_emails,
        gmail_group=gmail_group,
        folder="INBOX",
        start_date=start_date,
        chucksize=chucksize,
    )
    return uids | get_attachments_by_uid(
        storage_path=storage_path,
        filter_by_mime_type=filter_by_mime_type,
    )


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
    chucksize: int = DEFAULT_CHUNK_SIZE,
) -> TDataItem:
    """Collects email message UIDs (Unique IDs) from the mailbox.

    Args:
        host (str, optional): The hostname of the IMAP server. Default is 'dlt.secrets.value'.
        email_account (str, optional): The email account used to log in to the IMAP server. Default is 'dlt.secrets.value'.
        password (str, optional): The password for the email account. Default is 'dlt.secrets.value'.
        filter_emails (Sequence[str], optional): A sequence of email addresses used to filter emails based on the 'FROM' field. Default is 'FILTER_EMAILS' from settings.
        gmail_group (str, optional): The email address of the Google Group to filter emails sent to the group. Default is 'GMAIL_GROUP' from settings.
        folder (str, optional): The mailbox folder from which to collect emails. Default is 'INBOX'.
        start_date (pendulum.DateTime, optional): The start date from which to collect emails. Default is 'DEFAULT_START_DATE' from settings.
        initial_message_num (Optional[dlt.sources.incremental[int]], optional): The initial value for the incremental message UID. Default is 1.
        chucksize (int, optional): The number of message UIDs to collect at a time. Default is 'DEFAULT_CHUNK_SIZE' from settings.

    Yields:
        TDataItem: A dictionary containing the 'message_uid' of the collected email message.
    """

    last_message_num = initial_message_num.last_value

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        client.select(folder, readonly=True)

        criteria = [
            f"(SINCE {start_date.strftime('%d-%b-%Y')})",
            f"(UID {str(int(last_message_num))}:*)",
            f"(X-GM-RAW has:attachment)",
        ]

        if gmail_group:
            logger.info(f"Load all emails for Group: {gmail_group}")
            criteria.extend([f"(TO {gmail_group})"])

        if filter_emails:
            logger.info(f"Load emails only from: {filter_emails}")
            if len(filter_emails) == 1:
                filter_emails = filter_emails[0]
            if isinstance(filter_emails, str):
                criteria.append(f"(FROM {filter_emails})")
            else:
                email_filter = " ".join([f"FROM {email}" for email in filter_emails])
                criteria.append(f"(OR {email_filter})")

        uids = get_message_uids(client, criteria)
        for i in range(0, len(uids), chucksize):
            yield uids[i : i + chucksize]

class ImapSource(FilesystemSource):
    pass

@dlt.transformer(
    name="attachments",
    write_disposition="merge",
    merge_key="data_hash",
    primary_key="data_hash",
)
def get_attachments_by_uid(
    items: TDataItems,
    storage_path: str,
    host: str = dlt.secrets.value,
    email_account: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    filter_by_mime_type: Sequence[str] = (),
) -> TDataItem:
    """Downloads attachments from email messages based on the provided message UIDs.

    Args:
        items (TDataItems): An iterable containing dictionaries with 'message_uid' representing the email message UIDs.
        storage_path (str): The local folder path where attachments will be downloaded.
        host (str, optional): The hostname of the IMAP server. Default is 'dlt.secrets.value'.
        email_account (str, optional): The email account used to log in to the IMAP server. Default is 'dlt.secrets.value'.
        password (str, optional): The password for the email account. Default is 'dlt.secrets.value'.
        filter_by_mime_type (Sequence[str], optional): A sequence of MIME types used to filter attachments based on their content type. Default is an empty sequence.

    Yields:
        TDataItem: A dictionary containing the collected email information and attachment details.
    """

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        client.select()

        for item in items:
            message_uid = str(item["message_uid"])
            msg = get_message(client, message_uid)
            attachments = extract_attachments(msg, filter_by_mime_type)
            email_info = extract_email_info(msg)

            for attachment in attachments:
                file_data = ImapSource(
                    file_name=attachment["file_name"],
                    storage_path=storage_path,
                    file=attachment["payload"],
                    mod_date=email_info["Date"],
                    mime_type=attachment["content_type"],
                    remote_id=message_uid,
                )

                metadata = deepcopy(item)
                metadata.update(email_info)
                file_data.metadata = metadata

                yield file_data.dict()
