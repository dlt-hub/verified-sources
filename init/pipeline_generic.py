import dlt
from dlt.sources.helpers import requests


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {
        "Authorization": f"Bearer {api_secret_key}"
    }
    return headers


@dlt.source
def source(explicit_arg, api_url=dlt.config.value, api_secret_key=dlt.secrets.value, default_arg="default"):
    # as an example this source groups two resources that will be loaded together
    return resource_1(explicit_arg, api_url, api_secret_key), resource_2(api_url, api_secret_key, default_arg=default_arg)


@dlt.resource
def resource_1(explicit_arg, api_url=dlt.config.value, api_secret_key=dlt.secrets.value):
    headers = _create_auth_headers(api_secret_key)

    # uncomment line below to see if your headers are correct (ie. include valid api_key)
    # print(headers)
    # print(api_url)

    # make a call to the endpoint with request library
    resp = requests.get("%s?query=%s" % (api_url, explicit_arg), headers=headers)
    resp.raise_for_status()
    # yield the data from the resource
    data = resp.json()

    # yield a list of items
    yield data["items"]


@dlt.resource
def resource_2(api_url=dlt.config.value, api_secret_key=dlt.secrets.value, default_arg="default"):
    headers = _create_auth_headers(api_secret_key)

    # make a call to the endpoint with request library
    resp = requests.get("%s?last_value=%s" % (api_url, default_arg), headers=headers)
    resp.raise_for_status()
    # yield the data from the resource
    data = resp.json()

    # yield item by item
    for value in data["data"]:
        yield value


if __name__ == '__main__':
    # specify the pipeline name, destination and dataset name when configuring pipeline, otherwise the defaults will be used that are derived from the current script name
    p = dlt.pipeline(pipeline_name="generic", destination="bigquery", dataset_name="generic_data", full_refresh=False)

    # uncomment line below to execute the resource function and see the returned data
    # print(list(resource_1("term")))

    # explain that api_key will be automatically loaded from secrets.toml or environment variable below
    load_info = p.run(
        source("term", default_arg="last_value")
    )
    #pretty print the information on data that was loaded
    print(load_info)
