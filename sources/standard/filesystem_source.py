import abc
import hashlib
import mimetypes
import os
from typing import Any, Dict, Optional

from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TAnyDateTime


class FilesystemSource(metaclass=abc.ABCMeta):
    """Abstract base class for all filesystem sources."""

    metadata: Dict[str, Any] = {}

    def __init__(
        self,
        file_name: str,
        storage_path: str,
        file: bytearray,
        mod_date: Optional[TAnyDateTime] = None,
        mime_type: Optional[str] = None,
        remote_id: Optional[str] = None,
    ):
        self.storage_path = storage_path
        self.source_file_name = file_name
        self.remote_id = remote_id
        self.file_path = os.path.join(storage_path, self.file_name)
        self.mime_type = mime_type or self.extract_mimetype

        mod_date = mod_date or pendulum.now()
        self.mod_date = self.convert_mod_date(mod_date)
        self.file_data = file
        self.write_file()

    @property
    def file_name(self) -> str:
        """Get the file_name of a file."""
        if self.remote_id:
            return f"{self.remote_id}_{self.source_file_name}"
        return self.source_file_name

    def convert_mod_date(self, mod_date: TAnyDateTime) -> pendulum.DateTime:
        """Convert the modification date to a pendulum Datetime."""
        return ensure_pendulum_datetime(mod_date).in_tz("UTC")

    def write_file(self) -> None:
        """Write a file to the storage folder."""
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

        with open(self.file_path, "wb") as f:
            f.write(self.file_data)

    def extract_mimetype(self) -> str:
        """Get the mime type of a file."""
        return mimetypes.guess_type(self.file_path)[0] or "text/text"

    @property
    def file_hash(self) -> str:
        """Get the hash of a file."""
        return hashlib.sha256(self.file_data).hexdigest()

    def as_dict(self) -> Dict[str, Any]:
        file_dict = self.metadata.copy()
        file_dict.update(
            {
                "file_name": self.file_name,
                "file_path": os.path.abspath(self.file_path),
                "content_type": self.mime_type,
                "modification_date": self.mod_date,
                "data_hash": self.file_hash,
            }
        )
        return file_dict
