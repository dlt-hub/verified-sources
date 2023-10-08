import email
import imaplib
import hashlib
from email.message import Message
from email.header import decode_header, make_header
from time import mktime
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple

from dlt.common import logger, pendulum
from dlt.sources import TDataItems
from dlt.sources.filesystem import FileItem


class ImapFileItem(FileItem):
    """A Imap file item."""

    file_hash: str


def decode_header_word(v: Any) -> Any:
    if not isinstance(v, str):
        return v
    try:
        v = str(make_header(decode_header(v)))
    except Exception:
        pass

    return v


def get_message_uids(
    client: imaplib.IMAP4_SSL, criterias: Sequence[str]
) -> Optional[TDataItems]:
    """Get the message uids from the imap server.

    Args:
        client (imaplib.IMAP4_SSL): The imap client.
        criterias (Sequence[str]): The search criterias.

    Returns:
        Optional[TDataItems]: The list of message uids.
    """
    status, messages = client.uid("search", *criterias)

    if status != "OK":
        raise client.error(messages[-1])

    message_uids = messages[0].split()

    if not message_uids:
        logger.warning("No emails found.")
        return None

    return [{"message_uid": int(message_uid)} for message_uid in message_uids]


def get_internal_date(client: imaplib.IMAP4_SSL, message_uid: str) -> Optional[Any]:
    """Get the internal date of the email message.

    Parameters:
        client (imaplib.IMAP4_SSL): The imap client.
        message_uid (str): The uid of the message.

    Returns:
        Optional[Any]: The internal date of the email message.
    """
    # client.select()
    status, data = client.uid("fetch", message_uid, "(INTERNALDATE)")
    date = None

    if status != "OK":
        raise client.error(data[-1])

    timestruct = imaplib.Internaldate2tuple(data[0])
    date = pendulum.from_timestamp(mktime(timestruct))
    return date


def extract_email_info(msg: Message, include_body: bool = False) -> Dict[str, Any]:
    """Extract the email information from the email message.

    Parameters:
        msg (Message): The email message object.
        include_body (bool, optional): If true, the body of the email will be included.

    Returns:
        Dict[str, Any]: The email information.
    """
    email_data = dict(msg)
    email_data["Date"] = pendulum.parse(msg["Date"], strict=False)
    email_data["content_type"] = msg.get_content_type()
    if include_body:
        email_data["body"] = get_email_body(msg)

    return {
        k: decode_header_word(v)
        for k, v in email_data.items()
        if not k.startswith(("X-", "ARC-", "DKIM-"))
    }


def get_message_with_internal_date(
    client: imaplib.IMAP4_SSL, message_uid: str
) -> Tuple[Message, pendulum.DateTime]:
    """Get the email message and internal date from the imap server.

    Parameters:
        client (imaplib.IMAP4_SSL): The imap client.
        message_uid (str): The uid of the message.

    Returns:
        Tuple[Message, pendulum.DateTime]: The email message object and internal date as pendulum DateTime
    """
    status, data = client.uid("fetch", message_uid, "(RFC822 INTERNALDATE)")

    if status == "OK":
        try:
            raw_email = data[0][1]
        except (IndexError, TypeError):
            raise Exception(f"Error getting content of email with uid {message_uid}.")
    else:
        raise client.error(data[-1])

    msg = email.message_from_bytes(raw_email)
    timestruct = imaplib.Internaldate2tuple(data[1])
    return msg, pendulum.from_timestamp(mktime(timestruct))


def extract_attachments(
    message: Message, filter_by_mime_type: Sequence[str] = None
) -> Iterator[ImapFileItem]:
    """Extract the attachments from the email message.

    Parameters:
        message (Message): The email message object.
        filter_by_mime_type (str): The mime type to filter the attachments.

    Returns:
        Iterable[ImapFileItem]: The attachments.
    """
    for part in message.walk():
        content_type = part.get_content_type()
        content_disposition = part.get_content_disposition()

        # Checks if the content is an attachment
        if not content_disposition or content_disposition.lower() != "attachment":
            continue

        # Checks if the mime type is in the filter list
        if filter_by_mime_type and content_type not in filter_by_mime_type:
            continue

        file_md = ImapFileItem(  # type: ignore
            file_name=part.get_filename(),
            mime_type=content_type,
            file_content=part.get_payload(decode=True),
        )
        file_md["file_hash"] = hashlib.sha256(file_md["file_content"]).hexdigest()
        file_md["size_in_bytes"] = len(file_md["file_content"])

        yield file_md


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
