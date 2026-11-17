"""Large seeded resources read by the extraction benchmark, one per source"""

from functools import partial
from typing import Any, Callable, Dict, Literal, NamedTuple, Tuple

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.sources import DltResource

from sources.jira import jira_issues
from sources.salesforce import salesforce_table
from sources.salesforce.helpers.query import TQueryApi
from sources.servicenow import servicenow_table
from sources.servicenow.helpers.client import ServiceNowClient
from sources.sharepoint import sharepoint_list

from tests.jira.utils import PERF_ISSUES, PERF_PROJECTS, perf_jql
from tests.salesforce.seed import data as salesforce_data
from tests.salesforce.utils import performance_credentials
from tests.servicenow.seed import data as servicenow_data
from tests.servicenow.utils import get_credentials, wait_for_instance
from tests.sharepoint.seed import data as sharepoint_data
from tests.sharepoint.utils import graph_client

TLoad = Literal["full", "incremental"]
LOADS: Tuple[TLoad, ...] = ("full", "incremental")


class BenchmarkCase(NamedTuple):
    name: str
    source: str
    """Folder of the source in `sources`"""
    account: str
    """`account_lock` key shared with the performance tests and seeds of the account"""
    resource: Callable[[TLoad], DltResource]
    """Returns a new resource with default settings, without incremental for a full load"""
    expected_rows: int
    is_configured: Callable[[], bool]
    prepare: Callable[[], None]
    """Runs before the measured run, e.g. wakes up an instance"""
    loads: Tuple[TLoad, ...] = LOADS


def resolves(resolve: Callable[[], object]) -> bool:
    try:
        resolve()
    except (ConfigFieldMissingException, KeyError):
        return False
    return True


def nothing() -> None:
    pass


def load_args(load: TLoad) -> Dict[str, Any]:
    """A full load reads all records without the default incremental"""
    return {"incremental": None} if load == "full" else {}


def salesforce_resource(load: TLoad, api: TQueryApi = "auto") -> DltResource:
    return salesforce_table(
        credentials=performance_credentials(),
        sobject=salesforce_data.PERF_OBJECTS[0],
        api=api,
        **load_args(load),
    )


def servicenow_resource(load: TLoad) -> DltResource:
    return servicenow_table(table=servicenow_data.PERF_TABLES[0], **load_args(load))


def wake_servicenow() -> None:
    wait_for_instance(ServiceNowClient(get_credentials()))


def jira_resource(load: TLoad) -> DltResource:
    project = PERF_PROJECTS[0]
    return jira_issues(jql=perf_jql(project), name=project.lower(), **load_args(load))


def sharepoint_resource(load: TLoad) -> DltResource:
    # the delta query of a list has no full load, its first run reads all items
    return sharepoint_list(sharepoint_data.PERF_LIST)


def jira_credentials() -> None:
    for key in ("subdomain", "email", "api_token"):
        dlt.secrets[f"sources.jira.{key}"]


CASES: Dict[str, BenchmarkCase] = {
    case.name: case
    for case in (
        BenchmarkCase(
            "salesforce",
            "salesforce",
            salesforce_data.PERF_SECTION,
            salesforce_resource,
            salesforce_data.PERF_RECORDS,
            partial(resolves, performance_credentials),
            nothing,
        ),
        BenchmarkCase(
            "salesforce_rest",
            "salesforce",
            salesforce_data.PERF_SECTION,
            partial(salesforce_resource, api="rest"),
            salesforce_data.PERF_RECORDS,
            partial(resolves, performance_credentials),
            nothing,
        ),
        BenchmarkCase(
            "servicenow",
            "servicenow",
            "servicenow",
            servicenow_resource,
            servicenow_data.PERF_RECORDS,
            partial(resolves, get_credentials),
            wake_servicenow,
        ),
        BenchmarkCase(
            "jira",
            "jira",
            "jira",
            jira_resource,
            PERF_ISSUES,
            partial(resolves, jira_credentials),
            nothing,
        ),
        BenchmarkCase(
            "sharepoint",
            "sharepoint",
            "sharepoint",
            sharepoint_resource,
            sharepoint_data.PERF_ITEMS,
            partial(resolves, graph_client),
            nothing,
            ("incremental",),
        ),
    )
}
