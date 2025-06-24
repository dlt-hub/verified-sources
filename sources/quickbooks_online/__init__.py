"""Source for Quickbooks depending on the quickbooks_online-python python package.

Quickbooks-python docs: https://github.com/ej2/python-quickbooks
Quickbooks api docs: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/
Quickbooks company id: https://quickbooks.intuit.com/learn-support/en-uk/help-article/customer-company-settings/find-quickbooks-online-company-id/L7lp8O9yU_GB_en_GB
To get API credentials: https://developer.intuit.com/app/developer/qbo/docs/get-started/start-developing-your-app
Get oAuth Authorization code from: https://developer.intuit.com/app/developer/playground
"""

from dlt.sources import DltResource

from typing import Iterable, Sequence

import dlt
from .oauth_setup import QuickBooksAuth
from .settings import sandbox_env, production_env
from dlt.common.typing import TDataItem
from intuitlib.client import AuthClient
from quickbooks import QuickBooks
from quickbooks.objects.customer import Customer
from quickbooks.objects.invoice import Invoice


@dlt.source(name="quickbooks_online")
def quickbooks_online(
    environment: str,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    company_id: str = dlt.secrets.value,
    redirect_url: str = dlt.secrets.value,
) -> Sequence[DltResource]:
    """
    Retrieves data from Quickbooks using the Quickbooks API.

    Args:
        environment (str): The environment used for authentication, only "sandbox" or "production" values are allowed
        client_id (str): The client id provided by quickbooks for authentication. Defaults to the value in the `dlt.secrets` object.
        client_secret (str): The client secret provided by quickbooks for authentication. Defaults to the value in the `dlt.secrets` object.
        refresh_token (str): The refresh token given a quickbooks scope. Defaults to the value in the `dlt.secrets` object.
        company_id (str): The company id / realm id provided by quickbooks. Defaults to the value in the `dlt.secrets` object.
        redirect_url (str): The redirect uri end user creates in quickbooks, found in the developer application created. Defaults to the value in the `dlt.secrets` object.
    Yields:
        DltResource: Data resources from Quickbooks.
    """

    bearer_access_token = QuickBooksAuth(
        client_id=client_id,
        client_secret=client_secret,
        company_id=company_id,
        redirect_url=redirect_url,
        refresh_token=refresh_token,
        is_sandbox=False if environment == production_env else True,
    ).get_bearer_token_from_refresh_token()

    auth_client = AuthClient(
        client_id=client_id,
        client_secret=client_secret,
        environment=environment,
        redirect_uri=redirect_url,
        access_token=bearer_access_token.accessToken,
    )

    client = QuickBooks(
        auth_client=auth_client, refresh_token=refresh_token, company_id=company_id
    )

    # define resources
    @dlt.resource
    def customer() -> Iterable[TDataItem]:
        customer = Customer.all(qb=client)  # returns a list of iterables
        for record in customer:
            yield record.to_dict()

    @dlt.resource
    def invoice() -> Iterable[TDataItem]:
        invoice = Invoice.all(qb=client)
        for record in invoice:
            yield record.to_dict()

    return [customer, invoice]
