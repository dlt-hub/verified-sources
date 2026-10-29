"""Seeds the Salesforce test org and captures Bulk API 2.0 output as unit test fixtures.

Run from the repository root. Credentials are resolved from `sources/.dlt` like in tests:

    uv run python -m tests.salesforce.seed.seed [deploy|load|capture|all]

`deploy` creates the `DltTypes__c` and `DltPages__c` objects, the seed id fields on standard
objects and a permission set granting access to them. `load` upserts the records in `data.py` and
is safe to repeat. `capture` writes the describe and the gzipped CSV result page of `DltTypes__c`
to `tests/salesforce/cases`. See `tests/salesforce/README.md`.
"""

import gzip
import io
import json
import os
import sys
import zipfile
from datetime import date, datetime, time, timezone  # noqa: I251
from time import sleep
from typing import Any, Dict, List, cast
from xml.sax.saxutils import escape

# resolve configuration from sources/.dlt like the test harness
os.environ.setdefault("DLT_PROJECT_DIR", "sources")

import dlt
from dlt.common import Decimal
from dlt.common.configuration import with_config
from simple_salesforce import Salesforce
from simple_salesforce.api import SFType

from sources.salesforce.helpers.connection import SalesforceConnection
from sources.salesforce.helpers.credentials import (
    SalesforceAuth,
    make_salesforce_client,
)
from sources.salesforce.helpers.query import BulkQueryApi
from sources.salesforce.helpers.schema import fields_to_columns
from sources.salesforce.helpers.soql import make_soql

from tests.salesforce.seed import data

CASES_DIR = os.path.join(os.path.dirname(__file__), "..", "cases")
CAPTURED_CSV = os.path.join(CASES_DIR, "dlt_types__c.csv.gz")
CAPTURED_DESCRIBE = os.path.join(CASES_DIR, "dlt_types__c.describe.json")
METADATA_NS = "http://soap.sforce.com/2006/04/metadata"


@with_config(sections=("sources", "salesforce"))
def get_client(credentials: SalesforceAuth = dlt.secrets.value) -> Salesforce:
    return make_salesforce_client(credentials)


def to_xml(tag: str, value: Any) -> str:
    """Renders `value` as Metadata API XML. Elements are ordered like in the Metadata API XSD"""
    if isinstance(value, list):
        return "".join(to_xml(tag, item) for item in value)
    if isinstance(value, dict):
        keys = sorted(value, key=lambda key: (key != "fullName", key))
        inner = "".join(to_xml(key, value[key]) for key in keys)
    elif isinstance(value, bool):
        inner = "true" if value else "false"
    else:
        inner = escape(str(value))
    return f"<{tag}>{inner}</{tag}>"


def metadata_file(root: str, value: Dict[str, Any]) -> str:
    body = "".join(to_xml(key, value[key]) for key in sorted(value))
    return f'<?xml version="1.0" encoding="UTF-8"?><{root} xmlns="{METADATA_NS}">{body}</{root}>'


def build_package() -> bytes:
    """Builds the Metadata API deployment zip"""
    seed_id_field = {
        "fullName": data.SEED_ID_FIELD,
        "label": "dlt Seed Id",
        "type": "Text",
        "length": 64,
        "externalId": True,
        "unique": True,
    }
    custom_fields = [
        f"{obj}.{field['fullName']}"
        for obj, spec in data.CUSTOM_OBJECTS.items()
        for field in spec["fields"]
    ]
    custom_fields += [f"{obj}.{data.SEED_ID_FIELD}" for obj in data.SEED_ID_OBJECTS]
    files = {
        f"objects/{obj}.object": metadata_file(
            "CustomObject",
            {
                "deploymentStatus": "Deployed",
                "enableBulkApi": True,
                "enableSharing": True,
                "enableStreamingApi": True,
                "fields": spec["fields"],
                "label": spec["label"],
                "nameField": {"label": "Name", "type": "Text"},
                "pluralLabel": spec["label"],
                "sharingModel": "ReadWrite",
            },
        )
        for obj, spec in data.CUSTOM_OBJECTS.items()
    }
    files[f"permissionsets/{data.PERMISSION_SET}.permissionset"] = metadata_file(
        "PermissionSet",
        {
            "fieldPermissions": [
                {"editable": not _is_read_only(name), "field": name, "readable": True}
                for name in custom_fields
            ],
            "hasActivationRequired": False,
            "label": "dlt Seed",
            "objectPermissions": [
                {
                    "allowCreate": True,
                    "allowDelete": True,
                    "allowEdit": True,
                    "allowRead": True,
                    "modifyAllRecords": True,
                    "object": obj,
                    "viewAllRecords": True,
                }
                for obj in data.CUSTOM_OBJECTS
            ],
        },
    )
    files["package.xml"] = metadata_file(
        "Package",
        {
            "types": [
                {"members": list(data.CUSTOM_OBJECTS), "name": "CustomObject"},
                {
                    "members": [
                        f"{obj}.{data.SEED_ID_FIELD}" for obj in data.SEED_ID_OBJECTS
                    ],
                    "name": "CustomField",
                },
                {"members": data.PERMISSION_SET, "name": "PermissionSet"},
            ],
            "version": data.API_VERSION,
        },
    )
    for obj in data.SEED_ID_OBJECTS:
        files[f"objects/{obj}.object"] = metadata_file(
            "CustomObject", {"fields": seed_id_field}
        )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as package:
        for name, content in files.items():
            package.writestr(name, content)
    return buffer.getvalue()


def _is_read_only(field_name: str) -> bool:
    obj, name = field_name.split(".")
    fields = data.CUSTOM_OBJECTS.get(obj, {"fields": []})["fields"]
    field: Dict[str, Any] = next((f for f in fields if f["fullName"] == name), {})
    return "formula" in field or field.get("type") == "AutoNumber"


def deploy(client: Salesforce) -> None:
    result = client.deploy(io.BytesIO(build_package()), sandbox=False)
    while True:
        status = client.checkDeployStatus(result["asyncId"])
        if status["state"] in ("Succeeded", "Failed", "SucceededPartial", "Canceled"):
            break
        sleep(2)
    if status["state"] != "Succeeded":
        raise RuntimeError(
            f"Deployment {status['state']}: {status['deployment_detail']}"
        )
    print("Metadata deployed")

    user_id = client.session.get(
        f"https://{client.sf_instance}/services/oauth2/userinfo", headers=client.headers
    ).json()["user_id"]
    permission_set_id = client.query(
        f"SELECT Id FROM PermissionSet WHERE Name = '{data.PERMISSION_SET}'"
    )["records"][0]["Id"]
    assigned = client.query(
        "SELECT Id FROM PermissionSetAssignment"
        f" WHERE AssigneeId = '{user_id}' AND PermissionSetId = '{permission_set_id}'"
    )["totalSize"]
    if not assigned:
        cast(SFType, client.PermissionSetAssignment).create(
            {"AssigneeId": user_id, "PermissionSetId": permission_set_id}
        )
        print("Permission set assigned")
    # only marketing users can create campaigns
    cast(SFType, client.User).update(user_id, {"UserPermissionsMarketingUser": True})


def to_api_value(value: Any) -> Any:
    """Serializes python values in the REST API format"""
    if isinstance(value, dict):
        return {key: to_api_value(item) for key, item in value.items()}
    if isinstance(value, datetime):
        return (
            value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.strftime("%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(value, Decimal):
        return str(value)
    return value


def check_results(sobject: str, results: List[Dict[str, Any]]) -> None:
    errors = [result["errors"] for result in results if not result["success"]]
    if errors:
        raise RuntimeError(f"Writing {sobject} failed: {errors}")


def upsert(
    client: Salesforce, sobject: str, key: str, records: List[Dict[str, Any]]
) -> None:
    for start in range(0, len(records), 200):
        payload = [
            {"attributes": {"type": sobject}, **to_api_value(record)}
            for record in records[start : start + 200]
        ]
        results = client.restful(
            f"composite/sobjects/{sobject}/{key}",
            method="PATCH",
            json={"allOrNone": True, "records": payload},
        )
        check_results(sobject, results)
    print(f"Upserted {len(records)} {sobject}")


def create_missing(
    client: Salesforce, sobject: str, key: str, records: List[Dict[str, Any]]
) -> None:
    keys = ", ".join(f"'{record[key]}'" for record in records)
    existing = {
        record[key]
        for record in client.query_all(
            f"SELECT {key} FROM {sobject} WHERE {key} IN ({keys})"
        )["records"]
    }
    missing = [record for record in records if record[key] not in existing]
    if missing:
        payload = [
            {"attributes": {"type": sobject}, **to_api_value(record)}
            for record in missing
        ]
        results = client.restful(
            "composite/sobjects",
            method="POST",
            json={"allOrNone": True, "records": payload},
        )
        check_results(sobject, results)
    print(f"Created {len(missing)} of {len(records)} {sobject}")


def load(client: Salesforce) -> None:
    for sobject, records in data.UPSERTED.items():
        upsert(client, sobject, data.SEED_ID_FIELD, records)

    standard_pricebook = client.query(
        "SELECT Id, IsActive FROM Pricebook2 WHERE IsStandard = true"
    )["records"][0]
    if not standard_pricebook["IsActive"]:
        cast(SFType, client.Pricebook2).update(
            standard_pricebook["Id"], {"IsActive": True}
        )
    for sobject, spec in data.CREATED.items():
        records = [
            {
                name: standard_pricebook["Id"]
                if value == data.STANDARD_PRICEBOOK
                else value
                for name, value in record.items()
            }
            for record in spec["records"]
        ]
        create_missing(client, sobject, spec["key"], records)

    upsert(client, data.DLT_TYPES, "Seed_Id__c", data.DLT_TYPES_RECORDS)
    upsert(client, data.PAGES, "Seed_Id__c", data.PAGES_RECORDS)
    # deleted records go to the recycle bin, a unique seed id per run avoids collisions with them
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    deleted = [
        {
            "Seed_Id__c": f"deleted-{run_id}-{n}",
            "Name": f"deleted {n}",
            "Category__c": "deleted",
        }
        for n in range(1, data.DELETED_RECORDS + 1)
    ]
    upsert(client, data.DLT_TYPES, "Seed_Id__c", deleted)
    delete_where(client, data.DLT_TYPES, "Category__c = 'deleted'")

    prefix = data.DELETED_LINE_ITEM_PREFIX
    deleted_opportunity = {
        data.SEED_ID_FIELD: f"opp-deleted-{run_id}",
        "Name": "dlt deleted deal",
        "StageName": "Prospecting",
        "CloseDate": date(2026, 1, 31),
    }
    upsert(client, "Opportunity", data.SEED_ID_FIELD, [deleted_opportunity])
    line_items = [
        {
            data.SEED_ID_FIELD: f"{prefix}{kind}-{run_id}",
            "Opportunity": {data.SEED_ID_FIELD: opportunity},
            "PricebookEntry": {data.SEED_ID_FIELD: f"pbe-{data.STANDARD_PRICEBOOK}-1"},
            "Quantity": 1,
            "UnitPrice": 100,
        }
        for kind, opportunity in (
            ("direct", "opp-1"),
            ("cascade", deleted_opportunity[data.SEED_ID_FIELD]),
        )
    ]
    upsert(client, "OpportunityLineItem", data.SEED_ID_FIELD, line_items)
    # the opportunity deletes its line item, then the other one is deleted on its own
    delete_where(client, "Opportunity", f"{data.SEED_ID_FIELD} LIKE 'opp-deleted-%'")
    delete_where(
        client, "OpportunityLineItem", f"{data.SEED_ID_FIELD} LIKE '{prefix}%'"
    )


def delete_where(client: Salesforce, sobject: str, condition: str) -> None:
    ids = [
        record["Id"]
        for record in client.query_all(f"SELECT Id FROM {sobject} WHERE {condition}")[
            "records"
        ]
    ]
    results = client.restful(
        "composite/sobjects",
        method="DELETE",
        params={"ids": ",".join(ids), "allOrNone": "true"},
    )
    check_results(sobject, results)
    print(f"Deleted {len(ids)} {sobject}")


def capture(client: Salesforce) -> None:
    describe = getattr(client, data.DLT_TYPES).describe()
    fields = [
        {
            key: field[key]
            for key in ("name", "type", "precision", "scale", "nillable", "calculated")
        }
        for field in describe["fields"]
    ]
    columns = fields_to_columns(fields)
    pages = [
        page.csv
        for page in BulkQueryApi(SalesforceConnection(lambda: client, client)).query(
            make_soql(data.DLT_TYPES, columns),
            columns,
            query_all=True,
            page_size=10000,
        )
    ]
    assert len(pages) == 1, "all records are expected in a single page"
    os.makedirs(CASES_DIR, exist_ok=True)
    with open(CAPTURED_DESCRIBE, "w", encoding="utf-8") as f:
        json.dump(
            {"name": data.DLT_TYPES, "fields": fields}, f, indent=2, ensure_ascii=False
        )
        f.write("\n")
    with open(CAPTURED_CSV, "wb") as fb:
        # no timestamp in the gzip header so recapturing the same data gives the same file
        fb.write(gzip.compress(pages[0], mtime=0))
    print(f"Captured {len(fields)} fields and {len(pages[0])} bytes of CSV")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "all"
    # a new client per step: the Metadata API deploy leaves XML headers on the client
    if command in ("deploy", "all"):
        deploy(get_client())
    if command in ("load", "all"):
        load(get_client())
    if command in ("capture", "all"):
        capture(get_client())
