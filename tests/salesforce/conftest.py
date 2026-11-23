from typing import Iterator

import pytest
from dlt.common.configuration.exceptions import ConfigFieldMissingException

from sources.salesforce.helpers.credentials import (
    SalesforceAuth,
    make_salesforce_client,
)
from tests.salesforce.seed import data
from tests.salesforce.utils import performance_credentials
from tests.seed_utils import account_lock


@pytest.fixture
def perf_wide_account() -> Iterator[SalesforceAuth]:
    """Credentials of the performance org, held while a test reads its wide object. Skips when the
    org is not configured or the wide object is not seeded
    """
    try:
        credentials = performance_credentials()
    except ConfigFieldMissingException:
        pytest.skip(f"configure `sources.{data.PERF_SECTION}`, see README.md")
    with account_lock(data.PERF_SECTION):
        client = make_salesforce_client(credentials)
        count = client.query(f"SELECT COUNT() FROM {data.PERF_WIDE_OBJECT}")[
            "totalSize"
        ]
        if count != data.PERF_WIDE_RECORDS:
            pytest.skip(
                f"{data.PERF_WIDE_OBJECT} has {count} records, run the seed with --performance"
            )
        yield credentials
