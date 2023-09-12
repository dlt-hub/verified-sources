import email
import imaplib
from email.message import Message
from typing import Any, Dict, Optional, Sequence

from dlt.common import logger, pendulum
from dlt.extract.source import TDataItems


def get_message_uids(client: imaplib.IMAP4_SSL, criterias: Sequence[str]) -> Optional[TDataItems]:
    status, messages = client.uid("search", *criterias)

    if status != "OK":
        raise Exception("Error searching for emails.")

    message_uids = messages[0].split()

    if not message_uids:
        logger.warning("No emails found.")
        return None

    return [{"message_uid": int(message_uid)} for message_uid in message_uids]


def extract_email_info(msg: Message) -> Dict[str, Any]:
    email_data = dict(msg)
    email_data["Date"] = pendulum.parse(msg["Date"], strict=False)

    return {
        k: v for k, v in email_data.items() if not k.startswith(("X-", "ARC-", "DKIM-"))
    }


def get_message(client: imaplib.IMAP4_SSL, message_uid: str) -> Optional[Message]:

    status, data = client.uid("fetch", message_uid, "(RFC822)")
    if status == "OK":
        try:
            raw_email = data[0][1]
        except:
            raise Exception(f"Error getting content of email with uid {message_uid}.")
    else:
        raise Exception(f"Error fetching email with uid {message_uid}.")
    
    msg = email.message_from_bytes(raw_email)

    return msg


def extract_attachments(message: Message, filter_by_mime_type: str) -> Optional[Message]:

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

