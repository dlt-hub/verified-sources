"""This source collects inbox emails and downloads attachments to local folder"""
import hashlib
import imaplib
from copy import deepcopy
from itertools import chain
from typing import Iterable, List, Optional, Sequence

import dlt
from dlt.common import logger, pendulum
from dlt.sources import TDataItem, TDataItems, FileItem

from ..filesystem import FileSystemDict
from .helpers import (
    extract_attachments,
    extract_email_info,
    get_internal_date,
    get_message,
    get_message_uids,
)
from .settings import DEFAULT_CHUNK_SIZE, DEFAULT_START_DATE, FILTER_EMAILS, GMAIL_GROUP


class ImapFileItem(FileItem):
    """A Imap file item."""

    file_hash: str


@dlt.resource(selected=False)
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
    chunksize: int = DEFAULT_CHUNK_SIZE,
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
        chunksize (int, optional): The number of message UIDs to collect at a time. Default is 'DEFAULT_CHUNK_SIZE' from settings.

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
        for i in range(0, len(uids), chunksize):
            yield uids[i : i + chunksize]


@dlt.transformer(name="messages")
def get_message_content(
    items: TDataItems,
    host: str = dlt.secrets.value,
    email_account: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    include_body: bool = True,
) -> TDataItem:
    """Reads email messages from the mailbox based on the provided message UIDs.

    Args:
        items (TDataItems): An iterable containing dictionaries with 'message_uid' representing the email message UIDs.
        host (str, optional): The hostname of the IMAP server. Default is 'dlt.secrets.value'.
        email_account (str, optional): The email account used to log in to the IMAP server. Default is 'dlt.secrets.value'.
        password (str, optional): The password for the email account. Default is 'dlt.secrets.value'.
        include_body (bool, optional): If True, includes the email body in the result. Default is True.

    Yields:
        TDataItem: A dictionary containing the extracted email information from the read email message.
    """

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        client.select()

        for item in items:
            message_uid = str(item["message_uid"])
            msg = get_message(client, message_uid)
            if msg:
                result = deepcopy(item)
                result["modification_date"] = get_internal_date(client, message_uid)
                result.update(extract_email_info(msg, include_body=include_body))

                yield result


@dlt.transformer(
    name="attachments",
    write_disposition="merge",
    primary_key="data_hash",
)
def get_attachments(
    items: TDataItems,
    host: str = dlt.secrets.value,
    email_account: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    filter_by_mime_type: Sequence[str] = (),
    chunksize: int = DEFAULT_CHUNK_SIZE,
) -> Iterable[List[FileItem]]:
    """Downloads attachments from email messages based on the provided message UIDs.

    Args:
        items (TDataItems): An iterable containing dictionaries with 'message_uid' representing the email message UIDs.
        host (str, optional): The hostname of the IMAP server. Default is 'dlt.secrets.value'.
        email_account (str, optional): The email account used to log in to the IMAP server. Default is 'dlt.secrets.value'.
        password (str, optional): The password for the email account. Default is 'dlt.secrets.value'.
        filter_by_mime_type (Sequence[str], optional): A sequence of MIME types used to filter attachments based on their content type. Default is an empty sequence.
        chunksize (Iterable[List[FileSystemDict]]): The number of message UIDs to collect at a time. Default is 'DEFAULT_CHUNK_SIZE' from settings.

    Yields:
        Iterable[List[FileItem]]: A dictionary containing the attachment FileItem.
    """

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        client.select()

        for item in items:
            message_uid = str(item["message_uid"])
            msg = get_message(client, message_uid)
            if isinstance(filter_by_mime_type, str):
                filter_by_mime_type = [filter_by_mime_type]
            if filter_by_mime_type:
                attachments = None
                for mime_type in filter_by_mime_type:
                    new_attachments = extract_attachments(msg, mime_type)
                    if not attachments:
                        attachments = new_attachments
                    else:
                        attachments = chain(attachments, new_attachments)
            else:
                attachments = extract_attachments(msg)
            email_info = extract_email_info(msg)
            internal_date = get_internal_date(client, message_uid)

            files_dict: List[FileSystemDict] = []
            for attachment in attachments:
                filename = attachment["file_name"]

                file_hash = hashlib.sha256(attachment["payload"]).hexdigest()

                file_md = ImapFileItem(
                    file_name=filename,
                    file_url=f"imap://{email_account}/{message_uid}/{filename}",
                    mime_type=attachment["content_type"],
                    modification_date=internal_date,
                    file_content=attachment["payload"],
                    file_hash=file_hash,
                    size_in_bytes=len(attachment["payload"]),
                )

                file_dict = FileSystemDict(file_md)
                file_dict.update(email_info)
                file_dict.update(item)

                files_dict.append(file_dict)
                if len(files_dict) >= chunksize:
                    yield files_dict  # type: ignore
                    files_dict = []
        if files_dict:
            yield files_dict  # type: ignore
