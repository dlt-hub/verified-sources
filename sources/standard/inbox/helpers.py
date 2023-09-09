import email
import imaplib
from email.message import Message
from typing import Any, Dict, Optional

from dlt.common import pendulum


def extract_email_info(msg: Message, include_body: bool = False) -> Dict[str, Any]:
    email_data = dict(msg)
    email_data["Date"] = pendulum.parse(msg["Date"], strict=False)
    email_data["content_type"] = msg.get_content_type()
    if include_body:
        email_data["body"] = get_email_body(msg)

    return {
        k: v for k, v in email_data.items() if not k.startswith(("X-", "ARC-", "DKIM-"))
    }


def get_message_obj(client: imaplib.IMAP4_SSL, message_uid: str) -> Optional[Message]:
    client.select()

    status, data = client.uid("fetch", message_uid, "(RFC822)")
    msg = None
    if status == "OK":
        raw_email = data[0]
        if raw_email:
            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)

    return msg

# def extract_date(msg: Message) -> Optional[Any]:
#     date_format = "ddd, DD MMM YYYY HH:mm:ss ZZ"
#     date = next((hd[1] for hd in msg._headers if hd[0]=="Date"), None)
#     if date:
#         return pendulum.from_format(date, date_format).in_tz("UTC")

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
