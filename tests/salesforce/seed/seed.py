"""Seeds the Salesforce test org and captures Bulk API 2.0 output as unit test fixtures.

Run from the repository root. Credentials are resolved from `sources/.dlt` like in tests:

    uv run python -m tests.salesforce.seed.seed [deploy|load|capture|all]
    uv run python -m tests.salesforce.seed.seed [deploy|load|verify|all] --performance
    uv run python -m tests.salesforce.seed.seed jwt-app --certificate <cert.pem> [--name <app>] [--performance]

`deploy` creates the `DltTypes__c`, `DltPages__c` and `DltChanges__c` objects, the seed id fields
on standard objects and a permission set granting access to them. `load` upserts the records in
`data.py` and is safe to repeat. `capture` writes the describe and the gzipped CSV result page of
`DltTypes__c` to `tests/salesforce/cases`.

`--performance` seeds the performance org configured in `sources.salesforce_performance`: `deploy`
and `load` write the objects above and the performance objects in `data.PERF_OBJECTS`. `load`
ends with `verify`, which compares the number and cursor of every performance record with
`data.py`. an External Client App for the JWT bearer flow with the X.509 certificate of the client,
pre-authorized for the users with the performance permission set.
See `tests/salesforce/README.md`.
"""

import argparse
import gzip
import io
import json
import os
import zipfile
from datetime import date, datetime, time, timezone  # noqa: I251
from functools import partial
from time import sleep
from typing import Any, Callable, Dict, List, NamedTuple, Optional, cast
from xml.sax.saxutils import escape

# resolve configuration from sources/.dlt like the test harness
os.environ.setdefault("DLT_PROJECT_DIR", "sources")

import dlt
from dlt.common import Decimal
from dlt.common.configuration import with_config
from dlt.common.time import ensure_datetime_in_tz
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
from tests.seed_utils import (
    account_lock,
    raise_differences,
    record_http,
    report_differences,
)

CASES_DIR = os.path.join(os.path.dirname(__file__), "..", "cases")
CAPTURED_CSV = os.path.join(CASES_DIR, "dlt_types__c.csv.gz")
CAPTURED_DESCRIBE = os.path.join(CASES_DIR, "dlt_types__c.describe.json")
METADATA_NS = "http://soap.sforce.com/2006/04/metadata"
# more records are upserted with Bulk API 2.0
BULK_RECORDS = 10_000
# data storage of most records, the org refuses writes when its storage is full
RECORD_KB = 2
STORAGE_HEADROOM_MB = 5


class Package(NamedTuple):
    """Custom objects, the seed id field of standard objects and a permission set granting access
    to them
    """

    objects: Dict[str, Dict[str, Any]]
    seed_id_objects: List[str]
    permission_set: str
    label: str


PACKAGE = Package(
    data.CUSTOM_OBJECTS, data.SEED_ID_OBJECTS, data.PERMISSION_SET, "dlt Seed"
)
PERF_PACKAGE = Package(
    data.PERF_CUSTOM_OBJECTS, [], data.PERF_PERMISSION_SET, "dlt Seed Performance"
)


@with_config(sections=("sources", "salesforce"))
def get_client(credentials: SalesforceAuth = dlt.secrets.value) -> Salesforce:
    return make_salesforce_client(credentials)


@with_config(sections=("sources", data.PERF_SECTION))
def get_performance_client(
    credentials: SalesforceAuth = dlt.secrets.value,
) -> Salesforce:
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


def package_files(package: Package) -> Dict[str, str]:
    """Metadata API files of `package`, by path"""
    objects, seed_id_objects, permission_set, permission_set_label = package
    seed_id_field = {
        "fullName": data.SEED_ID_FIELD,
        "label": "dlt Seed Id",
        "type": "Text",
        "length": 64,
        "externalId": True,
        "unique": True,
    }
    field_permissions = [
        {
            "editable": not _is_read_only(field),
            "field": f"{obj}.{field['fullName']}",
            "readable": True,
        }
        for obj, spec in objects.items()
        for field in spec["fields"]
    ]
    field_permissions += [
        {"editable": True, "field": f"{obj}.{data.SEED_ID_FIELD}", "readable": True}
        for obj in seed_id_objects
    ]
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
        for obj, spec in objects.items()
    }
    files[f"permissionsets/{permission_set}.permissionset"] = metadata_file(
        "PermissionSet",
        {
            "fieldPermissions": field_permissions,
            "hasActivationRequired": False,
            "label": permission_set_label,
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
                for obj in objects
            ],
        },
    )
    types = [{"members": list(objects), "name": "CustomObject"}]
    if seed_id_objects:
        types.append(
            {
                "members": [f"{obj}.{data.SEED_ID_FIELD}" for obj in seed_id_objects],
                "name": "CustomField",
            }
        )
    types.append({"members": permission_set, "name": "PermissionSet"})
    files["package.xml"] = metadata_file(
        "Package", {"types": types, "version": data.API_VERSION}
    )
    for obj in seed_id_objects:
        files[f"objects/{obj}.object"] = metadata_file(
            "CustomObject", {"fields": seed_id_field}
        )
    return files


def _is_read_only(field: Dict[str, Any]) -> bool:
    return "formula" in field or field.get("type") == "AutoNumber"


def deploy_files(client: Salesforce, files: Dict[str, str]) -> None:
    """Deploys the metadata `files` by path and waits for the deployment"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as package:
        for name, content in files.items():
            package.writestr(name, content)
    buffer.seek(0)
    result = client.deploy(buffer, sandbox=False)
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


def user_info(client: Salesforce) -> Dict[str, Any]:
    response = client.session.get(
        f"https://{client.sf_instance}/services/oauth2/userinfo", headers=client.headers
    )
    response.raise_for_status()
    return response.json()  # type: ignore[no-any-return]


def deploy(client: Salesforce, package: Package = PACKAGE) -> None:
    """Deploys `package` and assigns its permission set to the user"""
    deploy_files(client, package_files(package))
    user_id = user_info(client)["user_id"]
    permission_set_id = client.query(
        f"SELECT Id FROM PermissionSet WHERE Name = '{package.permission_set}'"
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


def to_csv_value(value: Any) -> str:
    """Renders a value in the Bulk API CSV format, an empty value is null"""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(to_api_value(value))


def check_storage(client: Salesforce, sobject: str, records: int) -> None:
    """Raises when the `records` missing from `sobject` do not fit in the data storage left in the
    org
    """
    existing = client.query(f"SELECT COUNT() FROM {sobject}")["totalSize"]
    missing = max(records - existing, 0)
    storage = client.restful("limits")["DataStorageMB"]
    needed = missing * RECORD_KB / 1024
    print(
        f"{missing} new {sobject} need {needed:.0f} MB,"
        f" {storage['Remaining']} of {storage['Max']} MB left"
    )
    if needed > storage["Remaining"] - STORAGE_HEADROOM_MB:
        raise RuntimeError(
            f"{missing} new {sobject} do not fit in the data storage left in the org"
        )


def upsert(
    client: Salesforce, sobject: str, key: str, records: List[Dict[str, Any]]
) -> None:
    """Upserts `records` on `key`, more than `BULK_RECORDS` with Bulk API 2.0"""
    if len(records) > BULK_RECORDS:
        check_storage(client, sobject, len(records))
        bulk = getattr(client.bulk2, sobject)
        results = bulk.upsert(
            records=[
                {name: to_csv_value(value) for name, value in record.items()}
                for record in records
            ],
            external_id_field=key,
        )
        for result in results:
            if result["numberRecordsFailed"]:
                failed = bulk.get_failed_records(result["job_id"])
                raise RuntimeError(
                    f"{sobject}: {result['numberRecordsFailed']} records failed:\n"
                    + "\n".join(failed.splitlines()[:5])
                )
    else:
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


def deploy_jwt_app(client: Salesforce, certificate: str, app: str) -> None:
    """Deploys the External Client App `app` with the PEM `certificate` of a JWT bearer client"""
    user = user_info(client)
    common = {"externalClientApplication": app, "label": app}
    files = {
        f"externalClientApps/{app}.eca": metadata_file(
            "ExternalClientApplication",
            {
                "contactEmail": user["email"],
                "description": "JWT bearer flow for performance tests",
                "distributionState": "Local",
                "isProtected": False,
                "label": app,
            },
        ),
        f"extlClntAppOauthSettings/{app}_oauth.ecaOauth": metadata_file(
            "ExtlClntAppOauthSettings",
            {"commaSeparatedOauthScopes": "Api, RefreshToken", **common},
        ),
        f"extlClntAppGlobalOauthSets/{app}_glbloauth.ecaGlblOauth": metadata_file(
            "ExtlClntAppGlobalOauthSettings",
            {
                "callbackUrl": "http://localhost:1717/OauthRedirect",
                "certificate": certificate.strip(),
                "externalClientApplication": app,
                "isConsumerSecretOptional": False,
                "isIntrospectAllTokens": False,
                "isPkceRequired": False,
                "isSecretRequiredForRefreshToken": True,
                "label": app,
                "shouldRotateConsumerKey": False,
                "shouldRotateConsumerSecret": False,
            },
        ),
        f"extlClntAppPolicies/{app}_plcy.ecaPlcy": metadata_file(
            "ExtlClntAppConfigurablePolicies",
            {
                "externalClientApplication": app,
                "isEnabled": True,
                "isOauthPluginEnabled": True,
                "label": app,
                "startPage": "None",
            },
        ),
        f"extlClntAppOauthPolicies/{app}_oauthPlcy.ecaOauthPlcy": metadata_file(
            "ExtlClntAppOauthConfigurablePolicies",
            {
                "commaSeparatedPermissionSet": data.PERF_PERMISSION_SET,
                "externalClientApplication": app,
                "ipRelaxationPolicyType": "Enforce",
                "isClientCredentialsFlowEnabled": False,
                "label": app,
                "permittedUsersPolicyType": "AdminApprovedPreAuthorized",
                "refreshTokenPolicyType": "Infinite",
            },
        ),
    }
    files["package.xml"] = metadata_file(
        "Package",
        {
            "types": [
                {"members": app, "name": "ExternalClientApplication"},
                {"members": f"{app}_oauth", "name": "ExtlClntAppOauthSettings"},
                {
                    "members": f"{app}_glbloauth",
                    "name": "ExtlClntAppGlobalOauthSettings",
                },
                {"members": f"{app}_plcy", "name": "ExtlClntAppConfigurablePolicies"},
                {
                    "members": f"{app}_oauthPlcy",
                    "name": "ExtlClntAppOauthConfigurablePolicies",
                },
            ],
            "version": data.JWT_APP_API_VERSION,
        },
    )
    deploy_files(client, files)
    print(
        f"{app} is pre-authorized for {user['preferred_username']}, its consumer key is in"
        " Setup > External Client App Manager"
    )


def load_performance(client: Salesforce) -> None:
    for sobject in data.PERF_OBJECTS:
        upsert(client, sobject, "Seed_Id__c", data.perf_records(sobject))
    verify_performance(client)


def verify_performance(client: Salesforce) -> None:
    """Compares the number and cursor of the performance records with `data.py`"""
    errors: List[str] = []
    for sobject in data.PERF_OBJECTS:
        expected = {
            record["Seed_Id__c"]: (record["Number__c"], record[data.PERF_CURSOR])
            for record in data.perf_records(sobject)
        }
        stored = (
            (
                record["Seed_Id__c"],
                (
                    record["Number__c"],
                    ensure_datetime_in_tz(record[data.PERF_CURSOR]),
                ),
            )
            for record in client.query_all_iter(
                f"SELECT Seed_Id__c, Number__c, {data.PERF_CURSOR} FROM {sobject}"
            )
        )
        errors.extend(report_differences(sobject, expected, stored))
    raise_differences(errors)


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


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Seeds the Salesforce test org")
    parser.add_argument(
        "command",
        nargs="?",
        default="all",
        choices=["all", "deploy", "load", "capture", "verify", "jwt-app"],
    )
    parser.add_argument(
        "--performance",
        action="store_true",
        help="seed the performance org with the test data and the performance objects",
    )
    parser.add_argument(
        "--certificate",
        help="PEM file with the X.509 certificate of the JWT client (jwt-app)",
    )
    parser.add_argument(
        "--name", default=data.JWT_APP, help="name of the External Client App (jwt-app)"
    )
    args = parser.parse_args(argv)
    if args.command == "jwt-app":
        if not args.certificate:
            parser.error("`jwt-app` needs --certificate")
        with open(args.certificate, encoding="utf-8") as f:
            certificate = f.read()
        new_client = get_performance_client if args.performance else get_client
        deploy_jwt_app(new_client(), certificate, args.name)
        return
    if args.performance and args.command == "capture":
        parser.error("`capture` reads the test org, it needs no --performance")
    if not args.performance and args.command == "verify":
        parser.error("`verify` checks the performance objects, it needs --performance")
    new_client = get_performance_client if args.performance else get_client
    steps: Dict[str, List[Callable[[Salesforce], None]]] = {
        "deploy": (
            [deploy, partial(deploy, package=PERF_PACKAGE)]
            if args.performance
            else [deploy]
        ),
        "load": [load, load_performance] if args.performance else [load],
        "capture": [capture],
        "verify": [verify_performance],
    }
    if args.command == "all":
        commands = (
            ["deploy", "load"] if args.performance else ["deploy", "load", "capture"]
        )
    else:
        commands = [args.command]
    if args.performance:
        with account_lock(data.PERF_SECTION), record_http(
            f"seed {args.command} --performance"
        ):
            run_steps(new_client, steps, commands)
    else:
        run_steps(new_client, steps, commands)


def run_steps(
    new_client: Callable[[], Salesforce],
    steps: Dict[str, List[Callable[[Salesforce], None]]],
    commands: List[str],
) -> None:
    for command in commands:
        for step in steps[command]:
            # a new client per step: the Metadata API deploy leaves XML headers on the client
            step(new_client())


if __name__ == "__main__":
    main()
