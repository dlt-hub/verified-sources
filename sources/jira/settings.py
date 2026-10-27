from typing import Any, Dict, TypedDict


class _EndpointRequired(TypedDict):
    api_path: str
    key_field: str
    primary_key: str


class TEndpoint(_EndpointRequired, total=False):
    data_path: str
    offset_path: str
    page_size: int
    use_cursor_pagination: bool
    deduplicate: bool
    params: Dict[str, Any]


# `key_field` is the response field, `primary_key` the name dlt normalizes it to.
# `page_size` is the largest page the endpoint serves; the source argument overrides it.
DEFAULT_ENDPOINTS: Dict[str, TEndpoint] = {
    "issues": {
        "api_path": "rest/api/3/search/jql",
        "data_path": "issues",
        "use_cursor_pagination": True,
        "key_field": "id",
        "primary_key": "id",
        # jira caps the page at 100 issues while `fields` asks for `*all`
        "page_size": 100,
        "params": {
            "fields": "*all",
            "expand": "changelog,operations,transitions,names",
            "jql": "created >= '2000-01-01' order by created DESC",
        },
    },
    "users": {
        # bare array of active, inactive and previously deleted users, no envelope,
        # so the loop needs one empty page to stop
        "api_path": "rest/api/3/users",
        "key_field": "accountId",
        "primary_key": "account_id",
        "page_size": 1000,
        "params": {},
    },
    "workflows": {
        "api_path": "rest/api/3/workflows/search",
        "data_path": "values",
        "key_field": "id",
        "primary_key": "id",
        # jira caps the page at 50 workflows and reports no maximum
        "page_size": 50,
        "params": {},
    },
    "workflow_statuses": {
        # the names behind workflows__statuses.status_reference, under a sibling key of the
        # workflows response, so startAt advances on `values` and not on the rows taken
        "api_path": "rest/api/3/workflows/search",
        "data_path": "statuses",
        "offset_path": "values",
        "key_field": "statusReference",
        "primary_key": "status_reference",
        "page_size": 50,
        # a page repeats the statuses its workflows share
        "deduplicate": True,
        "params": {},
    },
    "projects": {
        "api_path": "rest/api/3/project/search",
        "data_path": "values",
        "key_field": "id",
        "primary_key": "id",
        # the documented maximum
        "page_size": 100,
        "params": {
            "expand": "description,lead,issueTypes,url,projectKeys,permissions,insight"
        },
    },
}
DEFAULT_PAGE_SIZE = 50
