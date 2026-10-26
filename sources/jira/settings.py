from typing import Any, Dict, TypedDict


class _EndpointRequired(TypedDict):
    api_path: str
    # the response field holding the key, which dlt normalizes into `primary_key`
    key_field: str
    primary_key: str


class TEndpoint(_EndpointRequired, total=False):
    data_path: str
    offset_path: str
    use_cursor_pagination: bool
    deduplicate: bool
    params: Dict[str, Any]


DEFAULT_ENDPOINTS: Dict[str, TEndpoint] = {
    "issues": {
        "data_path": "issues",
        "api_path": "rest/api/3/search/jql",
        "use_cursor_pagination": True,
        "key_field": "id",
        "primary_key": "id",
        "params": {
            "fields": "*all",
            "expand": "changelog,operations,transitions,names",
            "jql": "created >= '2000-01-01' order by created DESC",
        },
    },
    "users": {
        # returns a bare array of active, inactive and deleted users; maxResults is capped at 1000
        # no envelope, so the loop needs one empty page to stop
        "api_path": "rest/api/3/users",
        "key_field": "accountId",
        "primary_key": "account_id",
        "params": {},
    },
    "workflows": {
        # rest/api/3/workflow/search is deprecated and scheduled for removal
        "data_path": "values",
        "api_path": "rest/api/3/workflows/search",
        "key_field": "id",
        "primary_key": "id",
        "params": {},
    },
    "workflow_statuses": {
        # the names behind workflows__statuses.status_reference. Same response as workflows,
        # under a sibling key, so startAt advances on `values` and not on the rows taken.
        # rest/api/3/status omits unused statuses and rest/api/3/statuses/search omits
        # project scoped ones, so neither resolves every reference.
        "data_path": "statuses",
        "offset_path": "values",
        "api_path": "rest/api/3/workflows/search",
        "key_field": "statusReference",
        "primary_key": "status_reference",
        # pages repeat the statuses their workflows share. A merge disposition would
        # deduplicate them, but it would also collapse the rows whose key is null, which the
        # primary key hint allows, so the resource deduplicates and keeps `replace`.
        "deduplicate": True,
        "params": {},
    },
    "projects": {
        "data_path": "values",
        "api_path": "rest/api/3/project/search",
        "key_field": "id",
        "primary_key": "id",
        "params": {
            "expand": "description,lead,issueTypes,url,projectKeys,permissions,insight"
        },
    },
}
DEFAULT_PAGE_SIZE = 50
