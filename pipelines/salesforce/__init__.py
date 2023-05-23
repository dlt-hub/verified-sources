"""Pipeline for Salesforce depending on the simple_salesforce python package.

Imported resources are: account, campaign, contact, lead, opportunity, pricebook_2, pricebook_entry, product_2, user and user_role

Salesforce api docs: https://developer.salesforce.com/docs/apis

To get the security token: https://onlinehelp.coveo.com/en/ces/7.0/administrator/getting_the_security_token_for_your_salesforce_account.htm
"""

import typing as t
import pendulum

from typing import Any, Dict, Iterator, Iterator, Optional

import dlt
from simple_salesforce import Salesforce


def is_production() -> bool:
    """Return True if the current environment is production.

    You can override this function based on your own setup. It will
    add a LIMIT 100 clause to the queries.
    """
    return True

def get_records(
    sf: Salesforce,
    sobject: str,
    last_state: t.Optional[dict] = None,
    replication_key: t.Optional[str] = None,
) -> t.Iterable[dict]:

    # Get all fields for the sobject
    desc = getattr(sf, sobject).describe()
    # Salesforce returns compound fields as separate fields, so we need to filter them out
    compound_fields = {
        f["compoundFieldName"]
        for f in desc["fields"]
        if f["compoundFieldName"] is not None
    } - {"Name"}
    # Salesforce returns datetime fields as timestamps, so we need to convert them
    date_fields = {
        f["name"] for f in desc["fields"] if f["type"] in ("datetime",) and f["name"]
    }
    # If no fields are specified, use all fields except compound fields
    fields = [f["name"] for f in desc["fields"] if f["name"] not in compound_fields]

    # Generate a predicate to filter records by the replication key
    predicate, order_by, n_records = "", "", 0
    if replication_key:
        if last_state:
            predicate = f"WHERE {replication_key} > {last_state}"
        order_by = f"ORDER BY {replication_key} ASC"
    query = f"SELECT {', '.join(fields)} FROM {sobject} {predicate} {order_by}"
    if not is_production():
        query += " LIMIT 100"

    # Query all records in batches
    for page in getattr(sf.bulk, sobject).query_all(query, lazy_operation=True):
        for record in page:
            # Strip out the attributes field
            record.pop("attributes", None)
            for field in date_fields:
                # Convert Salesforce timestamps to ISO 8601
                if record.get(field):
                    record[field] = pendulum.from_timestamp(
                        record[field] / 1000,
                    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        yield page
        n_records += len(page)

@dlt.source(name="salesforce")
def salesforce_source(
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    security_token: str = dlt.secrets.value,
):
    client = Salesforce(username, password, security_token)

    # define resources
    @dlt.resource(write_disposition="replace")
    def sf_user() -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "User")

    @dlt.resource(write_disposition="replace")
    def user_role() -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "UserRole")

    @dlt.resource(write_disposition="merge")
    def opportunity(last_timestamp = dlt.sources.incremental("SystemModstamp", initial_value=None)) -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "Opportunity", last_timestamp.last_value, "SystemModstamp")

    @dlt.resource(write_disposition="merge")
    def opportunity_line_item(last_timestamp = dlt.sources.incremental("SystemModstamp", initial_value=None)) -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "OpportunityLineItem", last_timestamp.last_value, "SystemModstamp")

    @dlt.resource(write_disposition="merge")
    def opportunity_contact_role(last_timestamp = dlt.sources.incremental("SystemModstamp", initial_value=None)) -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "OpportunityContactRole", last_timestamp.last_value, "SystemModstamp")

    @dlt.resource(write_disposition="merge")
    def account(last_timestamp = dlt.sources.incremental("LastModifiedDate", initial_value=None)) -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "Account", last_timestamp.last_value, "LastModifiedDate")

    @dlt.resource(write_disposition="replace")
    def contact() -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "Contact")

    @dlt.resource(write_disposition="replace")
    def lead() -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "Lead")

    @dlt.resource(write_disposition="replace")
    def campaign() -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "Campaign")

    @dlt.resource(write_disposition="merge")
    def campaign_member(last_timestamp = dlt.sources.incremental("SystemModstamp", initial_value=None)) -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "CampaignMember", last_timestamp.last_value, "SystemModstamp")

    @dlt.resource(write_disposition="replace")
    def product_2() -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "Product2")

    @dlt.resource(write_disposition="replace")
    def pricebook_2() -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "Pricebook2")

    @dlt.resource(write_disposition="replace")
    def pricebook_entry() -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "PricebookEntry")

    @dlt.resource(write_disposition="merge")
    def task(last_timestamp = dlt.sources.incremental("SystemModstamp", initial_value=None)) -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "Task", last_timestamp.last_value, "SystemModstamp")

    @dlt.resource(write_disposition="merge")
    def event(last_timestamp = dlt.sources.incremental("SystemModstamp", initial_value=None)) -> Iterator[Dict[str, Any]]:
        yield from get_records(client, "Event", last_timestamp.last_value, "SystemModstamp")

    return (sf_user, user_role, opportunity,opportunity_line_item, opportunity_contact_role, account, contact, lead, campaign, campaign_member, product_2, pricebook_2,pricebook_entry, task, event)
