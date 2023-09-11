import email
import imaplib
from email.message import Message
from typing import Any, Dict, Optional

from dlt.common import pendulum


def extract_email_info(msg: Message) -> Dict[str, Any]:
    email_data = dict(msg)
    email_data["Date"] = pendulum.parse(msg["Date"], strict=False)
    email_data["content_type"] = msg.get_content_type()

    return {
        k: v for k, v in email_data.items() if not k.startswith(("X-", "ARC-", "DKIM-"))
    }


def get_message_attachment(client: imaplib.IMAP4_SSL, message_uid: str, filter_by_mime_type: str) -> Optional[Message]:
    client.select()

    status, data = client.uid("fetch", message_uid, "(RFC822)")
    if status == "OK":
        try:
            raw_email = data[0][1]
        except:
            raise Exception(f"Error getting content of email with uid {message_uid}.")
    else:
        raise Exception(f"Error fetching email with uid {message_uid}.")
    
    msg = email.message_from_bytes(raw_email)

    attachment_data = {}

    for part in msg.walk():

        content_type = part.get_content_type()
        filename = part.get_filename()
        if filter_by_mime_type and content_type not in filter_by_mime_type:
            continue

        attachment_data[filename] = part.get_payload(decode=True)


    return msg

