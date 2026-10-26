from typing import Any, Dict, TypedDict

from dlt.common.schema.typing import TWriteDisposition


class _EndpointRequired(TypedDict):
    api_path: str
    primary_key: str


class TEndpoint(_EndpointRequired, total=False):
    data_path: str
    offset_path: str
    use_cursor_pagination: bool
    write_disposition: TWriteDisposition
    deduplicate_on: str
    params: Dict[str, Any]


DEFAULT_ENDPOINTS: Dict[str, TEndpoint] = {
    "issues": {
        "data_path": "issues",
        "api_path": "rest/api/3/search/jql",
        "use_cursor_pagination": True,
        "primary_key": "id",
        # jira_search runs several queries and they match the same issue
        "deduplicate_on": "id",
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
    "workflow_statuses": {
        # the names behind workflows__statuses.status_reference. Same response as workflows,
        # under a sibling key, so startAt advances on `values` and not on the rows taken.
        # rest/api/3/status omits unused statuses and rest/api/3/statuses/search omits
        # project scoped ones, so neither resolves every reference.
        "data_path": "statuses",
        "offset_path": "values",
        "api_path": "rest/api/3/workflows/search",
        "primary_key": "status_reference",
        # pages repeat the statuses their workflows share
        "write_disposition": "merge",
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
