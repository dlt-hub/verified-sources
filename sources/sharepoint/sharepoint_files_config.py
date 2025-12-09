"""Configuration classes for SharePoint data extraction.

Provides configuration models for SharePoint lists and files,
including file type definitions and validation utilities.
"""
from typing import Optional, Dict
import re
from enum import Enum

import pandas as pd
from pydantic import BaseModel


class FileType(Enum):
    """Supported file types for SharePoint file extraction.

    Each file type maps to a corresponding pandas read function.

    Attributes:
        EXCEL: Excel files (.xlsx, .xls)
        CSV: Comma-separated values files
        JSON: JSON format files
        PARQUET: Apache Parquet files
        SAS: SAS data files
        SPSS: SPSS data files
        SAV: SPSS SAV format files
    """
    EXCEL = "excel"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    SAS = "sas"
    SPSS = "spss"
    SAV = "sav"

    def get_pd_function(self):
        """Get the pandas read function for this file type.

        Returns:
            Callable pandas read function (e.g., pd.read_csv, pd.read_excel)
        """
        return {
            self.EXCEL: pd.read_excel,
            self.CSV: pd.read_csv,
            self.JSON: pd.read_json,
            self.PARQUET: pd.read_parquet,
            self.SAS: pd.read_sas,
            self.SPSS: pd.read_spss,
        }[self]


class SharepointListConfig(BaseModel):
    """Configuration for SharePoint list data extraction.

    Attributes:
        table_name: Name of the destination table for the list data
        list_title: Display name of the SharePoint list to extract
        select: Optional comma-separated field names to retrieve
        is_incremental: Enable incremental loading (not yet implemented)

    Raises:
        NotImplementedError: If is_incremental is set to True
    """
    table_name: str
    list_title: str
    select: Optional[str] = None
    is_incremental: Optional[bool] = False

    def __init__(self, **data):
        super().__init__(**data)
        if self.is_incremental is True:
            raise NotImplementedError(
                "Incremental loading for Sharepoint List is not implemented yet."
            )


class SharepointFilesConfig(BaseModel):
    """Configuration for SharePoint file extraction and processing.

    Attributes:
        file_type: Type of files to process (CSV, Excel, etc.)
        folder_path: Path to the SharePoint folder containing files
        table_name: Name of the destination table for file data
        file_name_startswith: Prefix filter for file names
        pattern: Optional regex pattern for additional file filtering
        pandas_kwargs: Additional arguments to pass to pandas read function
        is_file_incremental: Enable incremental loading based on file modification time

    Note:
        The pattern attribute is automatically prefixed with file_name_startswith.
        Folder paths are validated and normalized during initialization.
    """
    file_type: FileType
    folder_path: str
    table_name: str
    file_name_startswith: str
    pattern: Optional[str] = ".*"
    pandas_kwargs: Dict = {}
    is_file_incremental: bool = False

    def __init__(self, **data):
        super().__init__(**data)
        self.folder_path = validate_folder_path(self.folder_path)
        self.pattern = f"^{self.file_name_startswith}{self.pattern}"


def validate_folder_path(folder_path: str) -> str:
    """Validate and normalize a SharePoint folder path.

    Removes leading/trailing slashes and validates that the path contains
    only allowed characters (alphanumeric, dashes, underscores, spaces, dots).

    Args:
        folder_path: The folder path to validate

    Returns:
        Normalized folder path without leading/trailing slashes

    Raises:
        ValueError: If path contains invalid characters or double slashes
    """
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
