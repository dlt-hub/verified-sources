import abc

class FilesystemSource(metaclass=abc.ABCMeta):
    """Abstract base class for all filesystem sources."""

    @abc.abstractmethod
    def __init__(self, storage_folder_path: str):
        pass