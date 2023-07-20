"""This resource collects attachments from Gmail inboxes to destinations"""
import os
from typing import Optional, Sequence, Dict, Any, Union
import dlt
from dlt.extract.source import TDataItem
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials

from .settings import HEADERS_DEFAULT, STORAGE_FOLDER_PATH
from .helpers import GmailClient


@dlt.resource(write_disposition="replace")
def gmail(
    credentials: Union[
        GcpOAuthCredentials, GcpServiceAccountCredentials
    ] = dlt.secrets.value,
    storage_folder_path: str = STORAGE_FOLDER_PATH,
    download: bool = False,
) -> TDataItem:

    client = GmailClient(credentials)

    messages_ids = client.messages_ids(user_id="me", message_type="inbox")

    if not messages_ids:
        print('No messages found in the inbox.')
    else:
        for message_id in messages_ids:
            content = client.message_content(user_id="me", message_id=message_id)

            response = {
                "message_id": message_id,
                "internal_date": content.get('internalDate'),
                "message_body": content.get('snippet'),
                "size_estimate": content.get('sizeEstimate')
            }

            payload = content.get('payload')

            parts = [payload]
            while parts:
                part = parts.pop()
                if part.get('parts'):
                    parts.extend(part['parts'])

                headers = part.get('headers')

                for header in headers:
                    name = header.get('name')
                    if name and name in HEADERS_DEFAULT:
                        response[name] = header.get('value')

                body = part.get('body')
                if part.get('filename'):
                    response["file_name"] = part['filename']
                    if 'data' in body:
                        file_data = client.encode_attachment_data(body['data'])
                    elif 'attachmentId' in body:
                        response["attachment_id"] = body['attachmentId']
                        attachment = client.attachments(user_id="me", message_id=message_id, attachment_id=body['attachmentId'])
                        file_data = client.encode_attachment_data(attachment['data'])
                    else:
                        file_data = None

                    if file_data and download:
                        response["file_path"] = os.path.abspath(save_path)
                        save_path = os.path.join(storage_folder_path, part['filename'])
                        os.makedirs(os.path.dirname(save_path), exist_ok=True)
                        with open(save_path, 'wb') as f:
                            f.write(file_data)

            yield response


