"""This source collects inbox emails and downloads attachments to local folder"""
import imaplib
import os
from copy import deepcopy
from typing import Optional, Sequence

import dlt
from dlt.common import logger, pendulum
from dlt.extract.source import DltResource, TDataItem, TDataItems

from .helpers import extract_email_info, get_message_obj, extract_date
from .settings import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_START_DATE,
    FILTER_EMAILS,
    GMAIL_GROUP,
    STORAGE_FOLDER_PATH,
)


@dlt.source
def inbox_source(
    storage_folder_path: str = STORAGE_FOLDER_PATH,
    gmail_group: Optional[str] = GMAIL_GROUP,
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
    filter_by_emails: Sequence[str] = FILTER_EMAILS,
    filter_by_mime_type: Sequence[str] = (),
    chucksize: int = DEFAULT_CHUNK_SIZE,
) -> DltResource:
    """This source collects inbox emails and downloads attachments to the local folder.

    Args:
        storage_folder_path (str, optional): The local folder path where attachments will be downloaded. Default is 'STORAGE_FOLDER_PATH' from settings.
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
        storage_folder_path=storage_folder_path,
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

        def get_message_uids(criterias: Sequence[str]) -> Optional[TDataItems]:
            status, messages = client.uid("search", *criterias)

            if status != "OK":
                raise Exception("Error searching for emails.")

            message_uids = messages[0].split()

            if not message_uids:
                logger.warning("No emails found.")
                return None
            else:
                return [
                    {"message_uid": int(message_uid)} for message_uid in message_uids
                ]

        if filter_emails:
            logger.info(f"Load emails only from: {filter_emails}")
            if len(filter_emails)==1:
                filter_emails = filter_emails[0]
            if isinstance(filter_emails, str):
                criteria.append(f"(FROM {filter_emails})")
            else:
                email_filter = " ".join([f"FROM {email}" for email in filter_emails])
                criteria.append(f"(OR {email_filter})")
        
        uids = get_message_uids(criteria)
        for i in range(0, len(uids), chucksize):
            yield uids[i:i + chucksize]


@dlt.transformer(
    name="attachments",
    write_disposition="merge",
    merge_key="data_hash",
    primary_key="data_hash",
)
def get_attachments_by_uid(
    items: TDataItems,
    storage_folder_path: str,
    host: str = dlt.secrets.value,
    email_account: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    include_body: bool = False,
    filter_by_mime_type: Sequence[str] = (),
) -> TDataItem:
    """Downloads attachments from email messages based on the provided message UIDs.

    Args:
        items (TDataItems): An iterable containing dictionaries with 'message_uid' representing the email message UIDs.
        storage_folder_path (str): The local folder path where attachments will be downloaded.
        host (str, optional): The hostname of the IMAP server. Default is 'dlt.secrets.value'.
        email_account (str, optional): The email account used to log in to the IMAP server. Default is 'dlt.secrets.value'.
        password (str, optional): The password for the email account. Default is 'dlt.secrets.value'.
        include_body (bool, optional): If True, includes the email body in the result. Default is False.
        filter_by_mime_type (Sequence[str], optional): A sequence of MIME types used to filter attachments based on their content type. Default is an empty sequence.

    Yields:
        TDataItem: A dictionary containing the collected email information and attachment details.
    """

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        client.select()

        for item in items:
            message_uid = str(item["message_uid"])
            msg = get_message_obj(client, message_uid)
            if msg:
                internal_date = extract_date(msg)
                email_info = extract_email_info(msg, include_body=include_body)

                for part in msg.walk():
                    content_disposition = part.get("Content-Disposition", "")
                    content_type = part.get_content_type()
                    if filter_by_mime_type and content_type not in filter_by_mime_type:
                        continue

                    if "attachment" in content_disposition:
                        filename = part.get_filename()
                        if filename:
                            attachment_data = part.get_payload(decode=True)
                            data_hash = hash(attachment_data)

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
                                    "content_type": content_type,
                                    "modification_date": internal_date,
                                    "data_hash": str(data_hash),
                                }
                            )
                            yield result
