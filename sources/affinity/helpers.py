"""Affinity CRM source helpers"""

from dlt.common.typing import StrAny
from dlt.sources.helpers import requests
from typing import NamedTuple

from .settings import API_BASE

class ListReference(NamedTuple):
    """ Represents a reference to an Affinity list, optionally a view """
    list_id: int
    """
    The ID of the list
    """
    view_id: int | None = None
    """
    The ID of the view
    """

    def __str__(self):
        return f"list-{self.list_id}" + (f"-{self.view_id}" if self.view_id else "")

    def __repr__(self):
        return f"<List: {self.list_id}, View: {self.view_id}>"

def generate_list_entries_path(list_ref: ListReference):
    is_view = list_ref.view_id is not None
    if is_view:
        return f"lists/{list_ref.list_id}/saved-views/{list_ref.view_id}/list-entries"
    else:
        return f"lists/{list_ref.list_id}/list-entries"
