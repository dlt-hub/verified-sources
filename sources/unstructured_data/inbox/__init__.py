"""This source collects inbox emails and downloads attachments to local folder"""
import imaplib
import os
from copy import deepcopy
from typing import Optional, Sequence

import dlt
from dlt.common import logger, pendulum
from dlt.extract.source import DltResource, TDataItem, TDataItems

from .helpers import extract_email_info, get_internal_date, get_message_obj
from .settings import (
    DEFAULT_START_DATE,
    FILTER_EMAILS,
    GMAIL_GROUP,
    STORAGE_FOLDER_PATH,
)


@dlt.source
def inbox_source(
    storage_folder_path: str = STORAGE_FOLDER_PATH,
    gmail_group: Optional[str] = GMAIL_GROUP,
    attachments: bool = False,
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
    filter_by_emails: Sequence[str] = FILTER_EMAILS,
    filter_by_mime_type: Sequence[str] = (),
) -> DltResource:
    """This source collects inbox emails and downloads attachments to the local folder.

    Args:
        storage_folder_path (str, optional): The local folder path where attachments will be downloaded. Default is 'STORAGE_FOLDER_PATH' from settings.
        gmail_group (str, optional): The email address of the Google Group to filter emails sent to the group. Default is 'GMAIL_GROUP' from settings.
        attachments (bool, optional): If True, downloads email attachments to the 'storage_folder_path'. Default is False.
        start_date (pendulum.DateTime, optional): The start date from which to collect emails. Default is 'DEFAULT_START_DATE' from settings.
        filter_by_emails (Sequence[str], optional): A sequence of email addresses used to filter emails based on the 'FROM' field. Default is 'FILTER_EMAILS' from settings.
        filter_by_mime_type (Sequence[str], optional): A sequence of MIME types used to filter attachments based on their content type. Default is an empty sequence.

    Returns:
        DltResource: A dlt resource containing the collected email information.
    """

    uids = messages_uids(
        filter_emails=filter_by_emails,
        gmail_group=gmail_group,
        folder="INBOX",
        start_date=start_date,
    )
    if attachments:
        return uids | get_attachments_by_uid(
            storage_folder_path=storage_folder_path,
            filter_by_mime_type=filter_by_mime_type,
        )
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

    Yields:
        TDataItem: A dictionary containing the 'message_uid' of the collected email message.
    """

    last_message_num = initial_message_num.last_value

    with imaplib.IMAP4_SSL(host) as client:
        client.login(email_account, password)
        client.select(folder, readonly=True)

        base_criteria = [
            f"(SINCE {start_date.strftime('%d-%b-%Y')})",
            f"(UID {str(int(last_message_num))}:*)",
        ]

        if gmail_group:
            logger.info(f"Load all emails for Group: {gmail_group}")
            base_criteria.extend([f"(TO {gmail_group})"])

        def get_message_uids(criterias: Sequence[str]) -> Optional[TDataItems]:
            status, messages = client.uid("search", *criterias)
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
            for email_ in filter_emails:
                criteria = base_criteria.copy()
                criteria.extend([f"(FROM {email_})"])
                yield get_message_uids(criteria)
        else:
            yield get_message_uids(base_criteria)


@dlt.transformer(name="messages")
def read_messages(
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
            msg = get_message_obj(client, message_uid)
            if msg:
                result = deepcopy(item)
                result["modification_date"] = get_internal_date(client, message_uid)
                result.update(extract_email_info(msg, include_body=include_body))

                yield result


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
                email_info = extract_email_info(msg, include_body=include_body)
                internal_date = get_internal_date(client, message_uid)

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
