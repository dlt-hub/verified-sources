import base64
from typing import Any, Union

from dlt.extract.source import TDataItem
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials
from googleapiclient.discovery import build


class GmailClient:
    def __init__(
        self, credentials: Union[GcpOAuthCredentials, GcpServiceAccountCredentials]
    ):
        if isinstance(credentials, GcpOAuthCredentials):
            credentials.auth("https://www.googleapis.com/auth/gmail.readonly")

        self.credentials = credentials
        self.service = build(
            "gmail", "v1", credentials=credentials.to_native_credentials()
        )
        self.messages = self.service.users().messages()

    def messages_info(
        self, user_id: str, message_type: str = "inbox", max_results: int = 100
    ) -> TDataItem:
        return self.messages.list(
            userId=user_id, maxResults=max_results, q=f"is:{message_type}"
        ).execute()

    def get_one_message(self, user_id: str, message_id: str) -> Any:
        return Message(gmail_client=self, user_id=user_id, message_id=message_id)


class Message:
    def __init__(self, gmail_client: GmailClient, user_id: str, message_id: str):
        self.client = gmail_client
        self.user_id = user_id
        self.message_id = message_id

    def content(self) -> TDataItem:
        return self.client.messages.get(
            userId=self.user_id, id=self.message_id
        ).execute()

    def attachments(self, attachment_id: str) -> TDataItem:
        return (
            self.client.messages.attachments()
            .get(userId=self.user_id, messageId=self.message_id, id=attachment_id)
            .execute()
        )

    @staticmethod
    def encode_attachment_data(data: str) -> bytes:
        return base64.urlsafe_b64decode(data.encode("UTF-8"))
