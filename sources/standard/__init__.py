"""A collection of standard sources for s3, gcs and azure buckets, inbox/imap etc."""

from .filesystem import filesystem_resource
from .inbox import messages_uids, get_attachments, get_message_content
