import dlt
from dlt.sources.helpers import requests
from urllib.parse import urlencode, urlunparse


# list of accounts ids to fetch workspaces from
TYPEFORM_ACCOUNTS_LIST = ["acc-id-1", "acc-id-2"]


@dlt.source
def typeform_source():
    """
    Retrieves data from the Typeform API for forms and workspaces.
    Returns:
        tuple: A tuple containing data for forms and workspaces.
    """
    return (typeform_forms(), typeform_workspaces())


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method.
    Args:
        api_secret_key (str): The API secret key.
    Returns:
        dict: The constructed authorization headers.
    """
    headers = {"Authorization": f"Bearer {api_secret_key}"}
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
    return url


def get_typeform_resource(
    endpoint, headers, page_size=1000, before=None, path=None, query=None
):
    """
    Retrieves data from the Typeform API for a specific endpoint.
    Args:
        endpoint (str): The Typeform API endpoint to retrieve data from.
        headers (dict): The authorization headers.
        page_size (int, optional): The number of results to fetch per page. Defaults to 1000.
        before (str, optional): A cursor that points to the start of the data to retrieve. Defaults to None.
        path (str, optional): The path for the URL. Defaults to None.
        query (dict, optional): Additional query parameters for the URL. Defaults to None.
    Yields:
        dict: The retrieved data in JSON format.
    """
    url = _build_url(
        "api.typeform.com",
        path if path else "",
        endpoint,
        query=query,
    )
    params = {}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    yield response.json()


@dlt.resource(write_disposition="append")
def typeform_forms(typeform_access_token=dlt.secrets.value):
    """
    Retrieves Typeform forms data using the Typeform API.
    Args:
        typeform_access_token (str): The Typeform API access token.
    Yields:
        dict: The retrieved forms data in JSON format.
    """
    headers = _create_auth_headers(typeform_access_token)
    yield from get_typeform_resource("forms", headers)


@dlt.resource(write_disposition="append")
def typeform_workspaces(typeform_access_token=dlt.secrets.value):
    """
    Retrieves Typeform Workspaces data for a given account.
    Args:
        typeform_access_token (str): The Typeform API access token.
    Yields:
        dict: The retrieved workspaces data in JSON format.
    """

    headers = _create_auth_headers(typeform_access_token)
    for acc in TYPEFORM_ACCOUNTS_LIST:
        yield from get_typeform_resource(f"{acc}/workspaces", headers, path="accounts/")


if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="typeform", destination="postgres", dataset_name="typeform_data"
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(typeform_source())

    # pretty print the information on data that was loaded
    print(load_info)
