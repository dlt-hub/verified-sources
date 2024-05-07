import dlt

from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth


@dlt.source
def source(
    explicit_arg,
    api_url=dlt.config.value,
    api_secret_key=dlt.secrets.value,
    default_arg="default",
):
    # as an example this source groups two resources that will be loaded together
    return resource_1(explicit_arg, api_url, api_secret_key), resource_2(
        api_url, api_secret_key, default_arg=default_arg
    )


@dlt.resource
def resource_1(
    explicit_arg, api_url=dlt.config.value, api_secret_key=dlt.secrets.value
):
    # uncomment line below to see if your headers are correct (ie. include valid api_key)
    # print(headers)
    # print(api_url)

    # Build rest client instance
    client = RESTClient(
        base_url=api_url,
        auth=BearerTokenAuth(api_secret_key),
    )

    # yield page by page
    for page in client.paginate(params={"query": explicit_arg}):
        yield page["items"]


@dlt.resource
def resource_2(
    api_url=dlt.config.value, api_secret_key=dlt.secrets.value, default_arg="default"
):
    # Build rest client instance
    client = RESTClient(
        base_url=api_url,
        auth=BearerTokenAuth(api_secret_key),
    )

    # yield item by item
    for page in client.paginate(params={"query": default_arg}):
        yield page["data"]


if __name__ == "__main__":
    # specify the pipeline name, destination and dataset name when configuring pipeline, otherwise the defaults will be used that are derived from the current script name
    p = dlt.pipeline(
        pipeline_name="generic",
        destination="bigquery",
        dataset_name="generic_data",
        full_refresh=False,
    )

    # uncomment line below to execute the resource function and see the returned data
    # print(list(resource_1("term")))

    # explain that api_key will be automatically loaded from secrets.toml or environment variable below
    load_info = p.run(source("term", default_arg="last_value"))
    # pretty print the information on data that was loaded
    print(load_info)
