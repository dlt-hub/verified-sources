from typing import Iterable, Dict, Any

import abc
from dlt import DltResource

class FilesystemSource(metaclass=abc.ABCMeta):
    """Abstract base class for all filesystem sources."""

    @abc.abstractmethod
    def __init__(self, storage_folder_path: str):
        pass

    @abc.abstractmethod
    def get_files(self, **kwargs) -> DltResource:
        """Get a dlt resource containing the files in the storage folder."""
        pass

    @abc.abstractmethod
    def list_remote_files(self, **kwargs) -> Iterable[Dict[str, Any]]:
        """Get a dlt resource containing the files in the storage folder."""
        pass