import dlt

from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

# This pipeline demonstrates how to use the dlt REST client for extracting data from the GitHub API.
# It showcases the use of authentication via bearer tokens and pagination for navigating through
# GitHub issues within a repository.

# Note: Ensure the API key (Bearer token) is set up correctly in secrets or environment variables.


@dlt.source
def source(
    api_url: str = dlt.config.value,
    api_secret_key: str = dlt.secrets.value,
    repository: str = dlt.config.value,
):
    return my_repo_issues(api_url, api_secret_key, repository)


@dlt.resource(write_disposition="append")
def my_repo_issues(
    api_url: str = dlt.config.value,
    api_secret_key: str = dlt.secrets.value,
    repository: str = dlt.config.value,
):
    # repository url should be in the format `OWNER/REPO`

    # paginate issues and yield every page
    url = f"{api_url}/repos/{repository}/issues"
    for page in paginate(
        url,
        auth=BearerTokenAuth(api_secret_key),
        # Note: for more paginators please see: https://dlthub.com/devel/general-usage/http/rest-client#paginators
        paginator=HeaderLinkPaginator(),
    ):
        print(page)
        yield page


if __name__ == "__main__":
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    pipeline = dlt.pipeline(
        pipeline_name="pipeline",
        destination="duckdb",
        dataset_name="pipeline_data",
    )

    data = list(my_repo_issues())

    # print the data yielded from resource
    print(data)
    exit()

    # run the pipeline with your parameters
    load_info = pipeline.run(source())

    # pretty print the information on data that was loaded
    print(load_info)
