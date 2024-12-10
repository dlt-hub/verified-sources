"""Chess source helpers"""

from dlt.common.typing import StrAny
from dlt.sources.helpers import requests
from typing import NamedTuple

from .settings import API_BASE

class ListReference(NamedTuple):
    list_id: int
    view_id: int | None = None

    def __str__(self):
        return f"list-{self.list_id}" + (f"-{self.view_id}" if self.view_id else "")

    def __repr__(self):
        return f"<List: {self.list_id}, View: {self.view_id}>"

def validate_month_string(string: str) -> None:
    """Validates that the string is in YYYY/MM format"""
    if string and string[4] != "/":
        raise ValueError(string)

def generate_list_entries_path(list_ref: ListReference):
    is_view = list_ref.view_id is not None
    if is_view:
        return f"lists/{list_ref.list_id}/saved-views/{list_ref.view_id}/list-entries"
    else:
        return f"lists/{list_ref.list_id}/list-entries"
