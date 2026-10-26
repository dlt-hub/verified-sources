from typing import Any, Dict, TypedDict


class _EndpointRequired(TypedDict):
    api_path: str
    primary_key: str


class TEndpoint(_EndpointRequired, total=False):
    data_path: str
    use_cursor_pagination: bool
    params: Dict[str, Any]


DEFAULT_ENDPOINTS: Dict[str, TEndpoint] = {
    "issues": {
        "data_path": "issues",
        "api_path": "rest/api/3/search/jql",
        "use_cursor_pagination": True,
        "primary_key": "id",
        "params": {
            "fields": "*all",
            "expand": "changelog,operations,transitions,names",
            "jql": "created >= '2000-01-01' order by created DESC",
        },
    },
    "users": {
        # returns a bare array of active, inactive and deleted users; maxResults is capped at 1000
        "api_path": "rest/api/3/users",
        "primary_key": "account_id",
        "params": {},
    },
    "workflows": {
        # rest/api/3/workflow/search is deprecated and scheduled for removal
        "data_path": "values",
        "api_path": "rest/api/3/workflows/search",
        "primary_key": "id",
        "params": {},
    },
    "projects": {
        "data_path": "values",
        "api_path": "rest/api/3/project/search",
        "primary_key": "id",
        "params": {
            "expand": "description,lead,issueTypes,url,projectKeys,permissions,insight"
        },
    },
}
DEFAULT_PAGE_SIZE = 50
