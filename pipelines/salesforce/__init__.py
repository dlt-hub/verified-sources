"""Pipeline for Salesforce depending on the simple_salesforce python package.

Imported resources are: account, campaign, contact, lead, opportunity, pricebook_2, pricebook_entry, product_2, user and user_role

Salesforce api docs: https://developer.salesforce.com/docs/apis

To get the security token: https://onlinehelp.coveo.com/en/ces/7.0/administrator/getting_the_security_token_for_your_salesforce_account.htm
"""

import typing as t
from pendulum import datetime

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
    state: t.Optional[dict] = None,
    replication_key: t.Optional[str] = None,
    fields: t.Optional[t.List[str]] = None,
) -> t.Iterable[dict]:
    """Return an iterable of records from the Salesforce API."""
    print(f"Starting {sobject} extraction...")
    assert not (
        (state is not None) ^ bool(replication_key)
    ), f"State and replication key must be used together: {state=}, {replication_key=}"
    if state:
        print("Init state:", state)

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
    if not fields:
        fields = [f["name"] for f in desc["fields"] if f["name"] not in compound_fields]

    # Generate a predicate to filter records by the replication key
    predicate, order_by, n_records = "", "", 0
    if replication_key:
        bookmark = state.get("last_replication_key")
        if bookmark:
            predicate = f"WHERE {replication_key} > {bookmark}"
        order_by = f"ORDER BY {replication_key} ASC"
    query = f"SELECT {', '.join(fields)} FROM {sobject} {predicate} {order_by}"
    if not is_production():
        query += " LIMIT 100"

    # Query all records in batches
    print(query)
    for page in getattr(sf.bulk, sobject).query_all(query, lazy_operation=True):
        for record in page:
            # Strip out the attributes field
            record.pop("attributes", None)
            for field in date_fields:
                # Convert Salesforce timestamps to ISO 8601
                if record.get(field):
                    record[field] = datetime.utcfromtimestamp(
                        record[field] / 1000,
                    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        yield page
        n_records += len(page)

        # Update the state with the last replication key
        if replication_key and page:
            # Assume the records are sorted by replication key
            state["last_replication_key"] = max(
                state.get("last_replication_key", ""),
                page[-1][replication_key],
            )

    print(f"Done with {sobject} extraction. {n_records} records extracted.")
    if state and n_records:
        print("New state:", state)


def sobject_factory(
    name: str,
    sobject: str,
    client: Salesforce,
    write_disposition: str = "replace",
    replication_key: t.Optional[str] = None,
    deferrable: bool = False,
    fields: t.Optional[t.List[str]] = None,
) -> t.Callable[[], t.Iterable[dict]]:
    """Return a resource for a Salesforce sobject.

    Args:
        name: The name of the resource. This affects the table name in the warehouse.
        sobject: The Salesforce object API name.
        client: A Salesforce client.
        write_disposition: The write disposition for the resource.
        replication_key: The field to use for incremental replication.
        deferrable: Whether the resource should be deferrable. We recommend not
            deferring large resources as it could cause memory issues.
        fields: A list of fields to extract. If not specified, all fields
            will be extracted.
    """

    @dlt.resource(name=name, write_disposition=write_disposition)
    def sobject_resource():
        nonlocal write_disposition, replication_key

        if replication_key and write_disposition == "append":
            state: dict = dlt.state().setdefault(name, {})
        else:
            replication_key = state = None

        if deferrable:
            yield dlt.defer(
                lambda: [
                    record
                    for page in get_records(
                        client, sobject, state, replication_key, fields
                    )
                    for record in page
                ]
            )
        else:
            yield from get_records(client, sobject, state, replication_key, fields)

    return sobject_resource


@dlt.source(name="salesforce")
def salesforce_source(
    username: str = dlt.config.value,
    password: str = dlt.secrets.value,
    security_token: str = dlt.secrets.value,
):
    client = Salesforce(username, password, security_token)
    # You can add more sobjects here, custom ones too
    # See standard objects here:
    # https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_list.htm
    # See the `sobject_factory` function docstring for info on the arguments
    return (
        sobject_factory(name="user", sobject="User", client=client, deferrable=True),
        sobject_factory(
            name="user_role", sobject="UserRole", client=client, deferrable=True
        ),
        sobject_factory(
            name="opportunity",
            sobject="Opportunity",
            client=client,
            write_disposition="append",
            replication_key="SystemModstamp",
        ),
        sobject_factory(
            name="opportunity_line_item",
            sobject="OpportunityLineItem",
            client=client,
            write_disposition="append",
            replication_key="SystemModstamp",
        ),
        sobject_factory(
            name="opportunity_contact_role",
            sobject="OpportunityContactRole",
            client=client,
            write_disposition="append",
            replication_key="SystemModstamp",
        ),
        sobject_factory(
            name="account",
            sobject="Account",
            client=client,
            write_disposition="append",
            replication_key="LastModifiedDate",
        ),
        sobject_factory(name="contact", sobject="Contact", client=client),
        sobject_factory(name="lead", sobject="Lead", client=client),
        sobject_factory(
            name="campaign", sobject="Campaign", client=client, deferrable=True
        ),
        sobject_factory(
            name="campaign_member",
            sobject="CampaignMember",
            client=client,
            write_disposition="append",
            replication_key="SystemModstamp",
        ),
        sobject_factory(
            name="product_2", sobject="Product2", client=client, deferrable=True
        ),
        sobject_factory(
            name="pricebook_2", sobject="Pricebook2", client=client, deferrable=True
        ),
        sobject_factory(
            name="pricebook_entry",
            sobject="PricebookEntry",
            client=client,
            deferrable=True,
        ),
        sobject_factory(
            name="task",
            sobject="Task",
            client=client,
            write_disposition="append",
            replication_key="SystemModstamp",
        ),
        sobject_factory(
            name="event",
            sobject="Event",
            client=client,
            write_disposition="append",
            replication_key="SystemModstamp",
        ),
    )