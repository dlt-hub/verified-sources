"""Contains all sources and resources for the Matomo pipeline."""
from typing import Iterator
import dlt
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItem
try:
    from matomoapi import Matomo
except ImportError:
    raise MissingDependencyException("Matomo API Client", ["google-api-python-client"])


@dlt.source(max_table_nesting=2)
def matomo(credentials: str = dlt.secrets.value) -> Iterator[TDataItem]:


    # Replace with your Matomo site URL and authentication token
    matomo_url = 'https://your-matomo-site.com'
    matomo_token = 'your-authentication-token'

    # Create an instance of the Matomo API client
    matomo = Matomo(matomo_url, token_auth=matomo_token)

    # Get the metadata for the available reports
    metadata = matomo.get_metadata()

    # Print the metadata
    for report in metadata:
        print(f"{report['name']}: {report['description']}")

    # Get the data for a specific report
    report_data = matomo.get_report('VisitsSummary.get', date='today', period='day')

    # Print the report data
    print(report_data)
