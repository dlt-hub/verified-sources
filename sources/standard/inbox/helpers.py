import email
import imaplib
from email.message import Message
from time import mktime
from typing import Any, Dict, Iterable, Optional, Sequence

from dlt.common import logger, pendulum
from dlt.extract.source import TDataItems


def get_message_uids(
    client: imaplib.IMAP4_SSL, criterias: Sequence[str]
) -> Optional[TDataItems]:
    status, messages = client.uid("search", *criterias)

    if status != "OK":
        raise Exception("Error searching for emails.")

    message_uids = messages[0].split()

    if not message_uids:
        logger.warning("No emails found.")
        return None

    return [{"message_uid": int(message_uid)} for message_uid in message_uids]


def get_internal_date(client: imaplib.IMAP4_SSL, message_uid: str) -> Optional[Any]:
    client.select()
    status, data = client.uid("fetch", message_uid, "(INTERNALDATE)")
    date = None
    if status == "OK":
        timestruct = imaplib.Internaldate2tuple(data[0])
        date = pendulum.from_timestamp(mktime(timestruct))
    return date


def extract_email_info(msg: Message, include_body: bool = False) -> Dict[str, Any]:
    email_data = dict(msg)
    email_data["Date"] = pendulum.parse(msg["Date"], strict=False)
    email_data["content_type"] = msg.get_content_type()
    if include_body:
        email_data["body"] = get_email_body(msg)

    return {
        k: v for k, v in email_data.items() if not k.startswith(("X-", "ARC-", "DKIM-"))
    }


def get_message(client: imaplib.IMAP4_SSL, message_uid: str) -> Optional[Message]:
    status, data = client.uid("fetch", message_uid, "(RFC822)")

    if status == "OK":
        try:
            raw_email = data[0][1]
        except (IndexError, TypeError):
            raise Exception(f"Error getting content of email with uid {message_uid}.")
    else:
        raise Exception(f"Error fetching email with uid {message_uid}.")

    msg = email.message_from_bytes(raw_email)

    return msg


def extract_attachments(
    message: Message, filter_by_mime_type: str
) -> Iterable[Dict[str, Any]]:
    for part in message.walk():
        content_type = part.get_content_type()
        content_disposition = part.get_content_disposition()

        # Checks if the content is an attachment
        if not content_disposition or content_disposition.lower() != "attachment":
            continue

        # Checks if the mime type is in the filter list
        if filter_by_mime_type and content_type not in filter_by_mime_type:
            continue

        attachment = {}
        attachment["content_type"] = content_type
        attachment["file_name"] = part.get_filename()
        attachment["payload"] = part.get_payload(decode=True)

        yield attachment


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
