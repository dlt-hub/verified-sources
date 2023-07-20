import base64
from typing import Optional, Sequence, Dict, Any, Union
from googleapiclient.discovery import build

from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials


class GmailClient:
    def __init__(self, credentials: Union[GcpOAuthCredentials, GcpServiceAccountCredentials]):
        if isinstance(credentials, GcpOAuthCredentials):
            credentials.auth("https://www.googleapis.com/auth/gmail.readonly")

        self.credentials = credentials
        self.service = build('gmail', 'v1', credentials=credentials.to_native_credentials())
        self.messages = self.service.users().messages()

    def messages_ids(self, user_id: str, message_type: str = "inbox", max_results: int = 100) -> Sequence[str]:
        messages_info = self.messages.list(userId=user_id, maxResults=max_results, q=f'is:{message_type}').execute()
        return [message['id'] for message in messages_info.get('messages', [])]

    def message_content(self, user_id: str, message_id: str,):
        return self.messages.get(userId=user_id, id=message_id).execute()

    def attachments(self, user_id: str, message_id: str, attachment_id: str):
        return self.messages.attachments().get(userId=user_id, messageId=message_id, id=attachment_id).execute()

    @staticmethod
    def encode_attachment_data(data: str):
        return base64.urlsafe_b64decode(data.encode('UTF-8'))
