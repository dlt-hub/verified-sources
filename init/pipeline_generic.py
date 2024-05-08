from typing import Any, Dict, Optional
import dlt

from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

# This pipeline demonstrates how to build a simple REST client for interacting with GitHub's API.
# It showcases the use of authentication via bearer tokens and pagination for navigating through
# GitHub issues and pull requests within a repository.

# Note: Ensure the API key (Bearer token) is set up correctly in secrets or environment variables.


@dlt.source
def source(
    api_url=dlt.config.value,
    api_secret_key=dlt.secrets.value,
    issues_params: Optional[Dict[str, Any]] = None,
    pulls_params: Optional[Dict[str, Any]] = None,
):
    """This source function aggregates data from two GitHub endpoints: issues and pull requests."""
    # Ensure that both the API URL and secret key are provided for GitHub
    # either via secrets.toml or via environment variables.
    return [
        dlt_repo_issues(api_url, api_secret_key, params=issues_params),
        dlt_repo_pulls(api_url, api_secret_key, params=pulls_params),
    ]


@dlt.resource
def dlt_repo_issues(
    api_url: str = dlt.config.value,
    api_secret_key: str = dlt.secrets.value,
    repository: str = dlt.config.value,
    params: Optional[Dict[str, Any]] = None,
):
    """
    Fetches issues from a specified repository on GitHub using Bearer Token Authentication.
    """
    # repository url should be in the format `OWNER/REPO`

    # Build rest client instance
    client = RESTClient(
        base_url=f"{api_url}/repos",
        auth=BearerTokenAuth(api_secret_key),
        paginator=HeaderLinkPaginator(),
    )

    # paginate issues and yield every page
    url = f"/{repository}/issues"
    for page in client.paginate(url, params=params):
        yield page


@dlt.resource
def dlt_repo_pulls(
    api_url: str = dlt.config.value,
    api_secret_key: str = dlt.secrets.value,
    repository: str = dlt.config.value,
    params: Optional[Dict[str, Any]] = None,
):
    # repository url should be in the format `OWNER/REPO`

    # Build rest client instance
    client = RESTClient(
        base_url=f"{api_url}/repos",
        auth=BearerTokenAuth(api_secret_key),
        paginator=HeaderLinkPaginator(),
    )

    # paginate pull requests and yield every page
    url = f"/{repository}/pulls"
    for page in client.paginate(url, params=params):
        yield page


if __name__ == "__main__":
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    p = dlt.pipeline(
        pipeline_name="generic",
        destination="duckdb",
        dataset_name="generic_data",
        full_refresh=False,
    )

    load_info = p.run(source(pulls_params={"state": "open"}))

    # pretty print the information on data that was loaded
    print(load_info)
