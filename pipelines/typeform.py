from urllib.parse import urlencode, urlunparse

import dlt
from dlt.sources.helpers import requests


@dlt.source
def typeform_source(typeform_access_token=dlt.secrets.value):
    return (
        get_typeform_resource(),
        get_workspace(),
        get_form_responses(),
        get_form_metadata(),
        get_insights(),
    )


def _create_auth_headers(typeform_access_token):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {"Authorization": f"Bearer {typeform_access_token}"}
    return headers


def _build_url(
    hostname: str,
    path: str,
    endpoint: str,
    scheme: str = "https",
    query: dict = None,
) -> str:
    """
    Builds a URL string using the given hostname, path, endpoint, scheme and query parameters.
    Args:
        hostname (str): The hostname for the URL.
        path (str): The path for the URL.
        endpoint (str): The endpoint for the URL.
        scheme (str, optional): The scheme for the URL. Defaults to "https".
        query (dict, optional): A dictionary containing query parameters for the URL. Defaults to None.
    Returns:
        str: A string representation of the built URL.
    """
    query = urlencode(query) if query is not None else None
    url = urlunparse((scheme, hostname, path + endpoint, "", query, ""))
    return


def get_typeform_resource(
    endpoint,
    page_size=1000,
    before=None,
    typeform_access_token=dlt.secrets.value,
):
    headers = _create_auth_headers(typeform_access_token)

    # check if authentication headers look fine
    print(headers)
    params = {"page_size": page_size}
    if before:
        params["before"] = before
    if start_date:
        params["since"] = start_date
    if end_date:
        params["until"] = end_date

    url = f"https://api.typeform.com/{endpoint}"

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    yield response.json()

    # # test data for loading validation, delete it once you yield actual data
    # test_data = [{"id": 0}, {"id": 1}]
    # yield test_data


@dlt.resource(write_disposition="append")
def get_workspace():
    """Get and load the Workspace and Forms data."""
    response = get_typeform_resource(endpoint="workspaces")

    yield response


@dlt.resource(write_disposition="append")
def get_form():
    """Get and load the Workspace and Forms data."""
    response = get_typeform_resource(endpoint="workspaces")

    yield response


@dlt.resource(write_disposition="append")
def get_form_metadata(form_id):
    """Get and load the Workspace and Forms data."""
    response = get_typeform_resource(endpoint=f"forms/{form_id}")

    yield response


@dlt.resource(write_disposition="append")
def get_insights():
    """Get and load the Workspace and Forms data."""
    response = get_typeform_resource(endpoint="workspaces")

    yield response


@dlt.resource(write_disposition="append")
def get_form_responses(form_id, page_size, before=None):
    """Get and load the Workspace and Forms data."""
    response = get_typeform_resource(
        endpoint=f"forms/{form_id}/responses", page_size=500, before=before
    )

    yield response


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="typeform",
        destination="postgres",
        dataset_name="typeform_data",
    )

    # print credentials by running the resource
    data = list(typeform_resource())

    # print the data yielded from resource
    print(data)
    # exit()

    # run the pipeline with your parameters
    load_info = pipeline.run(typeform_source())

    # pretty print the information on data that was loaded
    print(load_info)
