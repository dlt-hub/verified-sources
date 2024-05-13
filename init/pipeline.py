import dlt

from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

# This is a generic pipeline example and demonstrates
# how to use the dlt REST client for extracting data from APIs.
# It showcases the use of authentication via bearer tokens and pagination.


@dlt.source
def source(
    api_url: str = dlt.config.value,
    api_secret_key: str = dlt.secrets.value,
    repository: str = dlt.config.value,
):
    return resource(api_url, api_secret_key, repository)


@dlt.resource(write_disposition="append")
def resource(
    api_secret_key: str = dlt.secrets.value,
    org: str = "dlt-hub",
    repository: str = "dlt",
):
    # paginate issues and yield every page
    api_url = f"https://api.github.com/repos/{org}/{repository}/issues"
    for page in paginate(
        api_url,
        auth=BearerTokenAuth(api_secret_key),
        # Note: for more paginators please see:
        # https://dlthub.com/devel/general-usage/http/rest-client#paginators
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

    data = list(resource())

    # print the data yielded from resource
    print(data)

    # run the pipeline with your parameters
    # load_info = pipeline.run(source())

    # pretty print the information on data that was loaded
    # print(load_info)
