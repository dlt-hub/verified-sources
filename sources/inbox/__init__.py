"""Reads messages and attachments from e-mail inbox via IMAP protocol"""
import imaplib
from copy import deepcopy
from typing import Iterable, List, Optional, Sequence

import dlt
from dlt.common import logger, pendulum
from dlt.sources import TDataItem, TDataItems, DltResource
from dlt.sources.filesystem import FileItem, FileItemDict

from .helpers import (
    ImapFileItem,
    extract_attachments,
    extract_email_info,
    get_message_with_internal_date,
    get_message_uids,
)
from .settings import DEFAULT_CHUNK_SIZE, DEFAULT_START_DATE, GMAIL_GROUP


@dlt.source
def inbox_source(
    host: str = dlt.secrets.value,
    email_account: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    folder: str = "INBOX",
    gmail_group: Optional[str] = GMAIL_GROUP,
    start_date: pendulum.DateTime = DEFAULT_START_DATE,
    filter_emails: Sequence[str] = None,
    filter_by_mime_type: Sequence[str] = None,
    chunksize: int = DEFAULT_CHUNK_SIZE,
) -> Sequence[DltResource]:
    """This source collects inbox emails and downloads attachments to the local folder.

    Args:
        host (str, optional): The hostname of the IMAP server. Default is 'dlt.secrets.value'.
        email_account (str, optional): The email account used to log in to the IMAP server. Default is 'dlt.secrets.value'.
        password (str, optional): The password for the email account. Default is 'dlt.secrets.value'.
        folder (str, optional): The mailbox folder from which to collect emails. Default is 'INBOX'.
        gmail_group (str, optional): The email address of the Google Group to filter emails sent to the group. Default is 'GMAIL_GROUP' from settings.
        start_date (pendulum.Date, optional): The start date (with a day resolution) from which to collect emails. Default is 'DEFAULT_START_DATE' from settings.
        filter_emails (Sequence[str], optional): A sequence of email addresses used to filter emails based on the 'FROM' field. Default is 'FILTER_EMAILS' from settings.
        filter_by_mime_type (Sequence[str], optional): A sequence of MIME types used to filter attachments based on their content type. Default is an empty sequence.
        chunksize (int, optional): The number of message UIDs to collect at a time. Default is 'DEFAULT_CHUNK_SIZE' from settings.

    Returns:
        Sequence[DltResource]: Returns following resources: uids, messages, attachments
    """

    def _login(client: imaplib.IMAP4_SSL) -> None:
        client.login(email_account, password)
        r, dat = client.select(folder, readonly=True)
        if r != "OK":
            raise client.error(dat[-1])

    @dlt.resource(name="uids")
    def get_messages_uids(
        initial_message_num: Optional[
            dlt.sources.incremental[int]
        ] = dlt.sources.incremental("message_uid", initial_value=1),
    ) -> TDataItem:
        """Collects email message UIDs (Unique IDs) from the mailbox.
        Args:
            initial_message_num (int, optional): Controls incremental loading on UID

        Yields:
            TDataItem: A dictionary containing the 'message_uid' of the collected email message.
        """

        last_message_num = initial_message_num.last_value

        with imaplib.IMAP4_SSL(host) as client:
            _login(client)

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
                    criteria.append(f"(FROM {filter_emails[0]})")
                else:
                    email_filter = " ".join(
                        [f"FROM {email}" for email in filter_emails]
                    )
                    criteria.append(f"(OR {email_filter})")

            uids = get_message_uids(client, criteria)
            if uids:
                for i in range(0, len(uids), chunksize):
                    yield uids[i : i + chunksize]

    @dlt.transformer(name="messages", primary_key="message_uid")
    def get_messages(
        items: TDataItems,
        include_body: bool = True,
    ) -> TDataItem:
        """Reads email messages from the mailbox based on the provided message UIDs.

        Args:
            items (TDataItems): An iterable containing dictionaries with 'message_uid' representing the email message UIDs.
            include_body (bool, optional): If True, includes the email body in the result. Default is True.

        Yields:
            TDataItem: A dictionary containing the extracted email information from the read email message.
        """

        with imaplib.IMAP4_SSL(host) as client:
            _login(client)

            for item in items:
                message_uid = str(item["message_uid"])
                msg, internal_date = get_message_with_internal_date(client, message_uid)
                result = deepcopy(item)
                result["modification_date"] = internal_date
                result.update(extract_email_info(msg, include_body=include_body))

                yield result

    @dlt.transformer(
        name="attachments",
        primary_key="file_hash",
    )
    def get_attachments(
        items: TDataItems,
    ) -> Iterable[List[FileItem]]:
        """Downloads attachments from email messages based on the provided message UIDs.

        Args:
            items (TDataItems): An iterable containing dictionaries with 'message_uid' representing the email message UIDs.

        Yields:
            Iterable[List[FileItem]]: A dictionary containing the attachment FileItem.
        """

        with imaplib.IMAP4_SSL(host) as client:
            _login(client)

            files_dict: List[FileItemDict] = []

            for item in items:
                message_uid = str(item["message_uid"])
                msg, internal_date = get_message_with_internal_date(client, message_uid)
                attachments = list(extract_attachments(msg, filter_by_mime_type))
                if len(attachments) == 0:
                    continue

                email_info = extract_email_info(msg)

                for attachment in attachments:
                    attachment["modification_date"] = internal_date
                    attachment[
                        "file_url"
                    ] = f"imap://{email_account}/{message_uid}/{attachment['file_name']}"

                    file_dict = FileItemDict(attachment)
                    file_dict["message"] = dict(email_info)
                    file_dict["message"].update(item)

                    files_dict.append(file_dict)
                    if len(files_dict) >= chunksize:
                        yield files_dict  # type: ignore
                        files_dict = []

            # yield remainder
            if files_dict:
                yield files_dict  # type: ignore

    return (
        get_messages_uids,
        get_messages_uids | get_attachments,
        get_messages_uids | get_messages,
    )
