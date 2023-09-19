from typing import Optional, TypedDict

from pendulum import DateTime


class FileModel(TypedDict):
    file_name: str
    file_url: str
    content_type: str
    modification_date: DateTime
    size_in_bytes: int
