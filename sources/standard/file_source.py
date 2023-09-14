from pendulum import DateTime
from pydantic import BaseModel


class FileModel(BaseModel):
    file_name: str
    file_path: str
    content_type: str
    modification_date: DateTime
    data_hash: str
