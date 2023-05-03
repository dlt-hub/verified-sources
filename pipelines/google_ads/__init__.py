

from typing import Dict, List, Union
import dlt
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from pipelines.google_analytics.helpers.credentials import GoogleAnalyticsCredentialsOAuth
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import DictStrAny, DictStrStr, TDataItem
from dlt.common.pendulum import pendulum
from pipelines.google_ads.helpers.data_processing import process_dimension, process_report, process_query
try:
    from google.ads.googleads.client import GoogleAdsClient
except ImportError:
    raise MissingDependencyException("Requests-OAuthlib", ["google-ads"])
try:
    from google.oauth2.credentials import Credentials
except ImportError:
    raise MissingDependencyException("Google OAuth Library", ["google-auth-oauthlib"])


@dlt.source(max_table_nesting=2)
def google_ads_query(credentials: Union[GoogleAnalyticsCredentialsOAuth, GcpClientCredentialsWithDefault] = dlt.secrets.value,
                     dev_token: str = dlt.secrets.value,
                     query_list: List[DictStrAny] = dlt.config.value):
    """
    This source reads custom queries from the API.
    :param credentials:
    :param dev_token:
    :param query_list:
    :return:
    """

    # generate access token for credentials if we are using OAuth2.0
    if isinstance(credentials, GoogleAnalyticsCredentialsOAuth):
        credentials.auth("https://www.googleapis.com/auth/analytics.readonly")
        credentials = {
            "developer_token": dev_token,
            "refresh_token": credentials.refresh_token,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret
        }
        client = GoogleAdsClient.load_from_dict(config_dict=credentials)
    # use service account to authenticate if not using OAuth2.0
    else:
        pass

    resource_list = []
    for query in query_list:
        query_name = query["name"]
        query_sql = query["sql"]
        resource_list.append(
            dlt.resource(query_resource, name=query_name, write_disposition="append")(
                client=client,
                query=query_sql
            )
        )
    return resource_list


@dlt.source(max_table_nesting=2)
def google_ads(credentials: Union[GoogleAnalyticsCredentialsOAuth, GcpClientCredentialsWithDefault] = dlt.secrets.value,
               dev_token: str = dlt.secrets.value):
    """
    Loads default tables for google ads in the database.
    :param credentials:
    :param dev_token:
    :return:
    """
    # generate access token for credentials if we are using OAuth2.0
    if isinstance(credentials, GoogleAnalyticsCredentialsOAuth):
        credentials.auth("https://www.googleapis.com/auth/analytics.readonly")
        credentials = {
            "developer_token": dev_token,
            "refresh_token": credentials.refresh_token,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret
        }
        client = GoogleAdsClient.load_from_dict(config_dict=credentials)
    # use service account to authenticate if not using OAuth2.0
    else:
        pass

    resource_list = [get_dimensions(client=client), get_reports(client=client)]
    return resource_list


@dlt.resource(name="dimension_tables", write_disposition="replace")
def get_dimensions(client):
    """
    Dlt resource which loads dimensions.
    :param client:
    :return:
    """
    # Issues a search request using streaming.
    ga_service = client.get_service("GoogleAdsService")
    stream = ga_service.search_stream(customer_id="", query=query)
    for batch in stream:
        """
        Could be faster to just yield
        yield batch.results
        """
        processed_row_generator = process_dimension(batch=batch)
        yield from processed_row_generator


@dlt.resource(name="reports_table", write_disposition="append")
def get_reports(client):
    """
    Dlt resource which loads reports.
    :param client:
    :return:
    """
    # Issues a search request using streaming.
    ga_service = client.get_service("GoogleAdsService")
    stream = ga_service.search_stream(customer_id="", query=query)
    for batch in stream:
        """
        Could be faster to just yield
        yield batch.results
        """
        processed_row_generator = process_report(batch=batch)
        yield from processed_row_generator


def query_resource(client, query):
    """
    Dlt resource which loads queries.
    :param client:
    :param query:
    :return:
    """
    # Issues a search request using streaming.
    ga_service = client.get_service("GoogleAdsService")
    stream = ga_service.search_stream(customer_id="", query=query)
    for batch in stream:
        """
        Could be faster to just yield
        yield batch.results
        """
        processed_row_generator = process_query(batch=batch)
        yield from processed_row_generator
