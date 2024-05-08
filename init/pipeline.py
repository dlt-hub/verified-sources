import dlt

from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

# This pipeline demonstrates how to build a simple REST client for interacting with GitHub's API.
# It showcases the use of authentication via bearer tokens and pagination for navigating through
# GitHub issues and pull requests within a repository.

# Note: Ensure the API key (Bearer token) is set up correctly in secrets or environment variables.


@dlt.source
def source(api_url: str = dlt.config.value, api_secret_key: str = dlt.secrets.value):
    return my_repo_pulls(api_url, api_secret_key)


@dlt.resource(write_disposition="append")
def my_repo_pulls(
    api_url: str = dlt.config.value,
    api_secret_key=dlt.secrets.value,
    repository: str = dlt.config.value,
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
    for page in client.paginate(url):
        yield page


if __name__ == "__main__":
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    pipeline = dlt.pipeline(
        pipeline_name="pipeline",
        destination="duckdb",
        dataset_name="pipeline_data",
    )

    data = list(my_repo_pulls())

    # print the data yielded from resource
    print(data)
    exit()

    # run the pipeline with your parameters
    load_info = pipeline.run(source())

    # pretty print the information on data that was loaded
    print(load_info)
