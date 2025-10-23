from typing import Iterator, Optional, Sequence, List, Dict
import re
from enum import Enum

from loguru import logger
import pandas as pd
from pydantic import BaseModel



class FileType(Enum):
    EXCEL = "excel"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    SAS = "sas"
    SPSS = "spss"
    SAV = "sav"

    def get_pd_function(self):
        return {
            self.EXCEL: pd.read_excel,
            self.CSV: pd.read_csv,
            self.JSON: pd.read_json,
            self.PARQUET: pd.read_parquet,
            self.SAS: pd.read_sas,
            self.SPSS: pd.read_spss,
        }[self]


class SharepointListConfig(BaseModel):
    table_name: str
    list_title: str
    select: Optional[str] = None
    limit: Optional[int] = None
    is_incremental: Optional[bool] = False

    def __init__(self, **data):
        super().__init__(**data)
        if self.is_incremental is True:
            raise NotImplementedError(
                "Incremental loading for Sharepoint List is not implemented yet."
            )

class SharepointFilesConfig(BaseModel):
    file_type: FileType
    folder_path: str
    table_name: str
    file_name_startswith: str
    pattern: Optional[str] = ".*"
    pandas_kwargs: Dict = {}
    limit: Optional[int] = None
    file_size_limit: Optional[int] = 100_000_000  # 100 MB
    is_compressed_folder: Optional[bool] = False
    if_apply_str_to_all_columns: Optional[bool] = True
    is_file_incremental: bool = False

    def __init__(self, **data):
        super().__init__(**data)
        self.folder_path = validate_folder_path(self.folder_path)
        self.pattern = f"^{self.file_name_startswith}{self.pattern}"


def validate_folder_path(folder_path: str) -> str:
    if folder_path.startswith("/"):
        folder_path = folder_path[1:]
    if folder_path.endswith("/"):
        folder_path = folder_path[:-1]
    if not re.compile(r"^[a-zA-Z0-9_\-/\s\.]*$").match(folder_path):
        raise ValueError(
            "Invalid folder path, only alphanumeric characters, dashes and underscores are"
            f" allowed: {folder_path}"
        )
    if re.compile(r"//").search(folder_path):
        raise ValueError(f"Invalid folder path with double slashes: {folder_path}")
    return folder_path
