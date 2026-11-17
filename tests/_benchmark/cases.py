"""Large seeded resources read by the extraction benchmark, one per source"""

from functools import partial
from typing import Callable, Dict, NamedTuple

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.sources import DltResource

from sources.jira import jira_issues
from sources.salesforce import salesforce_table
from sources.salesforce.helpers.query import TQueryApi
from sources.servicenow import servicenow_table
from sources.servicenow.helpers.client import ServiceNowClient

from tests.jira.utils import PERF_ISSUES, PERF_PROJECTS, perf_jql
from tests.salesforce.seed import data as salesforce_data
from tests.salesforce.utils import performance_credentials
from tests.servicenow.seed import data as servicenow_data
from tests.servicenow.utils import get_credentials, wait_for_instance


class BenchmarkCase(NamedTuple):
    name: str
    account: str
    """`account_lock` key shared with the performance tests and seeds of the account"""
    resource: Callable[[], DltResource]
    """Returns a new resource with default settings"""
    expected_rows: int
    is_configured: Callable[[], bool]
    prepare: Callable[[], None]
    """Runs before the measured run, e.g. wakes up an instance"""


def resolves(resolve: Callable[[], object]) -> bool:
    try:
        resolve()
    except (ConfigFieldMissingException, KeyError):
        return False
    return True


def nothing() -> None:
    pass


def salesforce_resource(api: TQueryApi = "auto") -> DltResource:
    return salesforce_table(
        credentials=performance_credentials(),
        sobject=salesforce_data.PERF_OBJECTS[0],
        api=api,
    )


def servicenow_resource() -> DltResource:
    return servicenow_table(table=servicenow_data.PERF_TABLES[0])


def wake_servicenow() -> None:
    wait_for_instance(ServiceNowClient(get_credentials()))


def jira_resource() -> DltResource:
    project = PERF_PROJECTS[0]
    return jira_issues(jql=perf_jql(project), name=project.lower())


def jira_credentials() -> None:
    for key in ("subdomain", "email", "api_token"):
        dlt.secrets[f"sources.jira.{key}"]


CASES: Dict[str, BenchmarkCase] = {
    case.name: case
    for case in (
        BenchmarkCase(
            "salesforce",
            salesforce_data.PERF_SECTION,
            salesforce_resource,
            salesforce_data.PERF_RECORDS,
            partial(resolves, performance_credentials),
            nothing,
        ),
        BenchmarkCase(
            "salesforce_rest",
            salesforce_data.PERF_SECTION,
            partial(salesforce_resource, "rest"),
            salesforce_data.PERF_RECORDS,
            partial(resolves, performance_credentials),
            nothing,
        ),
        BenchmarkCase(
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
            jira_resource,
            PERF_ISSUES,
            partial(resolves, jira_credentials),
            nothing,
        ),
    )
}
