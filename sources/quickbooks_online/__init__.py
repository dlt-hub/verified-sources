"""Source for Quickbooks depending on the quickbooks_online-python python package.

Quickbooks-python docs: https://github.com/ej2/python-quickbooks
Quickbooks api docs: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/
Quickbooks company id: https://quickbooks.intuit.com/learn-support/en-uk/help-article/customer-company-settings/find-quickbooks-online-company-id/L7lp8O9yU_GB_en_GB
To get API credentials: https://developer.intuit.com/app/developer/qbo/docs/get-started/start-developing-your-app
Get oAuth Authorization code from: https://developer.intuit.com/app/developer/playground
"""

from dlt.sources import DltResource

from typing import Iterable

import dlt
from dlt.common.typing import TDataItem
from intuitlib.client import AuthClient
from quickbooks import QuickBooks
from quickbooks.objects.customer import Customer


@dlt.source(name="quickbooks_online")
def quickbooks_online(
    environment: str = dlt.secrets.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    access_token: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    company_id: str = dlt.secrets.value,
    redirect_uri: str = dlt.secrets.value,
) -> Iterable[DltResource]:
    """
    Retrieves data from Quickbooks using the Quickbooks API.

    Args:
        environment (str): The environment used for authentication ('sandbox' or 'production')
        client_id (str): The client id provided by quickbooks for authentication. Defaults to the value in the `dlt.secrets` object.
        client_secret (str): The client secret provided by quickbooks for authentication. Defaults to the value in the `dlt.secrets` object.
        access_token (str): The access token provided by quickbooks oAuth playground for authentication. Defaults to the value in the `dlt.secrets` object.
        refresh_token (str): The refresh token provided by quickbooks oAuth playground for authentication. Defaults to the value in the `dlt.secrets` object.
        company_id (str): The company id / realm id provided by quickbooks. Defaults to the value in the `dlt.secrets` object.
        redirect_uri (str): The redirect uri provided by quickbooks, found in the developer application created. Defaults to the value in the `dlt.secrets` object.
    Yields:
        DltResource: Data resources from Quickbooks.
    """
    auth_client = AuthClient(
        client_id=client_id,
        client_secret=client_secret,
        environment=environment,
        redirect_uri=redirect_uri,
        access_token=access_token,
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

    return customer
