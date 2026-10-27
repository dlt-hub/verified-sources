from typing import Any, Dict, TypedDict


class _EndpointRequired(TypedDict):
    api_path: str
    key_field: str
    primary_key: str


class TEndpoint(_EndpointRequired, total=False):
    data_path: str
    offset_path: str
    page_size: int
    deduplicate: bool
    params: Dict[str, Any]


# `key_field` is the response field, `primary_key` the name dlt normalizes it to.
# `page_size` is the largest page the endpoint serves; the source argument overrides it.
DEFAULT_ENDPOINTS: Dict[str, TEndpoint] = {
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

# api v2 returns rich text fields and worklog comments as strings, v3 as Atlassian Document Format
FIELDS_PATH = "rest/api/2/field"
MYSELF_PATH = "rest/api/2/myself"
SEARCH_PATH = "rest/api/2/search/jql"
ISSUE_BULKFETCH_PATH = "rest/api/2/issue/bulkfetch"
CHANGELOG_BULKFETCH_PATH = "rest/api/2/changelog/bulkfetch"
ISSUE_COMMENTS_PATH = "rest/api/2/issue/{issue_id}/comment"
WORKLOG_UPDATED_PATH = "rest/api/2/worklog/updated"
WORKLOG_DELETED_PATH = "rest/api/2/worklog/deleted"
WORKLOG_LIST_PATH = "rest/api/2/worklog/list"

# seconds re-read on each run: search results are eventually consistent and JQL compares
# datetimes with minute precision
DEFAULT_LAG = 600.0
# search rejects queries without a restriction
BOUNDING_JQL = 'created >= "1970-01-01"'
# max issues in a search page that requests ids only
MAX_SEARCH_PAGE_SIZE = 5000
# bulk fetch returns up to 1000 issues for an explicit list of max 100 single value fields,
# 100 issues otherwise
MAX_BULK_ISSUES = 1000
DEFAULT_BULK_ISSUES = 100
MAX_BULK_ISSUES_FIELDS = 100
MULTI_VALUE_FIELDS = {"comment", "worklog", "attachment"}
# loaded by dedicated resources, restrictions are permission metadata with dynamic keys
EXCLUDED_FIELDS = {"comment", "worklog", "issuerestriction"}
# min issues in a page yielded by the issues resource: pages are fetched in parallel and each
# page has a fixed cost on extract
DEFAULT_CHUNK_SIZE = 200
MAX_CHANGELOG_ISSUES = 1000
# max comments in a page of comments of an issue
MAX_COMMENTS_PAGE_SIZE = 5000
MAX_WORKLOG_IDS = 1000
