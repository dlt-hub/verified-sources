"""Seeds the ServiceNow test instance and captures its responses as test cases.

Run from the repository root. Credentials are resolved from `sources/.dlt` like in tests:

    uv run python -m tests.servicenow.seed.seed [deploy|load|capture|capture-hidden|all]
    uv run python -m tests.servicenow.seed.seed [deploy|load|verify|all] --performance

`deploy` creates the `u_dlt_types` table with a field of every type, the `u_dlt_pages`,
`u_dlt_incremental` and `u_dlt_changes` tables and the choices of `u_choice`. `load` deletes stale
`types-` records and upserts the records in `data.py` on their seed id. Then it creates and deletes
the records in `data.DELETED_SEED_IDS` once, so `sys_audit_delete` has records of `u_dlt_types`.
`capture` writes the `sys_dictionary` fields, a Table API page and a CSV export of `u_dlt_types` to
`tests/servicenow/cases`. `capture-hidden`, not part of `all`, writes a page and an export of
`u_dlt_pages` while a read ACL hides `sys_updated_on`.

With `--performance` the steps write only the performance tables in `data.PERF_TABLES`. `load`
ends with `verify`, which compares the number and cursor of every record with `data.py`.
"""

import argparse
import json
import os
from collections import defaultdict
from datetime import date, datetime, time, timezone  # noqa: I251
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple

# dlt resolves config and secrets from sources/.dlt, like in tests
os.environ.setdefault("DLT_PROJECT_DIR", "sources")

from dlt.common import Decimal

from sources.servicenow.helpers.client import (
    CSV_EXPORT_PARAMS,
    TABLE_API_PARAMS,
    ServiceNowClient,
)
from sources.servicenow.helpers.query import GLIDE_DATETIME_FORMAT, TableQuery

from tests.seed_utils import (
    account_lock,
    map_with_progress,
    raise_differences,
    record_http,
    report_differences,
)
from tests.servicenow.seed import data
from tests.servicenow.utils import get_credentials, wait_for_instance

# user names of the account are not captured
NOT_CAPTURED = {"sys_created_by", "sys_updated_by"}
# tables with their label and fields besides `data.SEED_ID_FIELD`
TABLES = {
    data.DLT_TYPES: ("dlt types", data.FIELDS),
    data.DLT_PAGES: ("dlt pages", data.PAGE_FIELDS),
    data.DLT_INCREMENTAL: ("dlt incremental", data.INCREMENTAL_FIELDS),
    data.DLT_CHANGES: ("dlt changes", data.CHANGES_FIELDS),
}
PERF_TABLES = {
    table: (f"dlt perf {table[-1]}", data.PERF_FIELDS) for table in data.PERF_TABLES
}
ALL_TABLES = {**TABLES, **PERF_TABLES}


def find_one(
    client: ServiceNowClient, table: str, query: str
) -> Optional[Dict[str, Any]]:
    return next(client.records(table, TableQuery([query]), ["sys_id"]), None)


def send(
    client: ServiceNowClient, method: str, path: str, **kwargs: Any
) -> Dict[str, Any]:
    response = client.rest.request(path, method=method, **kwargs)  # type: ignore[arg-type]
    response.raise_for_status()
    return response.json() if response.content else {}  # type: ignore[no-any-return]


def to_input(value: Any, field: str) -> str:
    """Renders a value in the database format the Table API accepts"""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime(GLIDE_DATETIME_FORMAT)
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, float):
        # the instance does not parse exponents, for example 1e-07
        return format(Decimal(repr(value)), "f")
    if data.FIELDS.get(field, {}).get("internal_type") == "glide_duration":
        return datetime.fromtimestamp(value, timezone.utc).strftime(
            GLIDE_DATETIME_FORMAT
        )
    return str(value)


def deploy_tables(
    client: ServiceNowClient, tables: Dict[str, Tuple[str, Dict[str, Dict[str, Any]]]]
) -> None:
    for table, (label, fields) in tables.items():
        if not find_one(client, "sys_db_object", f"name={table}"):
            send(
                client,
                "POST",
                "table/sys_db_object",
                json={"name": table, "label": label, "create_access_controls": "true"},
            )
        fields = {
            data.SEED_ID_FIELD: {"internal_type": "string", "max_length": 64},
            **fields,
        }
        for element, attributes in fields.items():
            if find_one(client, "sys_dictionary", f"name={table}^element={element}"):
                continue
            send(
                client,
                "POST",
                "table/sys_dictionary",
                json={
                    "name": table,
                    "element": element,
                    "column_label": element[2:].replace("_", " "),
                    **{key: str(value) for key, value in attributes.items()},
                },
            )


def deploy(client: ServiceNowClient) -> None:
    deploy_tables(client, TABLES)
    for value, label in data.CHOICES.items():
        query = f"name={data.DLT_TYPES}^element=u_choice^value={value}"
        if not find_one(client, "sys_choice", query):
            send(
                client,
                "POST",
                "table/sys_choice",
                json={
                    "name": data.DLT_TYPES,
                    "element": "u_choice",
                    "value": value,
                    "label": label,
                    "language": "en",
                },
            )


def insert(client: ServiceNowClient, table: str, record: Dict[str, Any]) -> str:
    values = {field: to_input(value, field) for field, value in record.items()}
    return str(send(client, "POST", f"table/{table}", json=values)["result"]["sys_id"])


def delete(client: ServiceNowClient, table: str, sys_id: str) -> None:
    send(client, "DELETE", f"table/{table}/{sys_id}")


def upsert(client: ServiceNowClient, table: str, record: Dict[str, Any]) -> str:
    """Creates or updates `record` matched on its seed id, returns its sys_id. Fields of the table
    missing from `record` are emptied.
    """
    values = {field: record.get(field) for field in ALL_TABLES[table][1]}
    values[data.SEED_ID_FIELD] = record[data.SEED_ID_FIELD]
    existing = find_one(
        client, table, f"{data.SEED_ID_FIELD}={record[data.SEED_ID_FIELD]}"
    )
    if existing:
        send(
            client,
            "PATCH",
            f"table/{table}/{existing['sys_id']}",
            json={field: to_input(value, field) for field, value in values.items()},
        )
        return str(existing["sys_id"])
    return insert(client, table, values)


def load(client: ServiceNowClient) -> None:
    seed_ids = {record[data.SEED_ID_FIELD] for record in data.RECORDS}
    stale = TableQuery([f"{data.SEED_ID_FIELD}STARTSWITHtypes-"])
    for record in list(
        client.records(data.DLT_TYPES, stale, ["sys_id", data.SEED_ID_FIELD])
    ):
        if record[data.SEED_ID_FIELD] not in seed_ids:
            delete(client, data.DLT_TYPES, record["sys_id"])
    for record in data.RECORDS:
        upsert(client, data.DLT_TYPES, record)
    if not find_one(client, "sys_audit_delete", f"tablename={data.DLT_TYPES}"):
        for seed_id in data.DELETED_SEED_IDS:
            sys_id = upsert(client, data.DLT_TYPES, {data.SEED_ID_FIELD: seed_id})
            delete(client, data.DLT_TYPES, sys_id)
    load_records(client, data.DLT_PAGES, data.page_records(), "u_when")
    load_records(
        client,
        data.DLT_INCREMENTAL,
        data.incremental_records() + [data.FUTURE_RECORD],
        data.INCREMENTAL_CURSOR,
    )


def load_records(
    client: ServiceNowClient, table: str, records: List[Dict[str, Any]], field: str
) -> None:
    """Deletes all but one record of each seed id, a retried insert may create more. Then inserts
    the `records` missing from `table` and updates the ones whose `field` differs
    """
    existing: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in client.records(
        table, TableQuery(), ["sys_id", data.SEED_ID_FIELD, field]
    ):
        existing[record[data.SEED_ID_FIELD]].append(record)
    duplicates = [record["sys_id"] for same in existing.values() for record in same[1:]]
    missing = [
        record for record in records if record[data.SEED_ID_FIELD] not in existing
    ]
    changed = [
        record
        for record in records
        if record[data.SEED_ID_FIELD] in existing
        and existing[record[data.SEED_ID_FIELD]][0].get(field, "")
        != to_input(record[field], field)
    ]
    map_with_progress(partial(delete, client, table), duplicates, f"{table} deletes")
    map_with_progress(partial(insert, client, table), missing, f"{table} inserts")
    map_with_progress(partial(upsert, client, table), changed, f"{table} updates")


def verify_records(
    client: ServiceNowClient,
    table: str,
    records: List[Dict[str, Any]],
    fields: List[str],
) -> List[str]:
    """Compares the `fields` of the records in `table` with `records` by seed id"""
    expected = {
        record[data.SEED_ID_FIELD]: [to_input(record[field], field) for field in fields]
        for record in records
    }
    stored = (
        (record[data.SEED_ID_FIELD], [record.get(field, "") for field in fields])
        for record in client.records(table, TableQuery(), [data.SEED_ID_FIELD, *fields])
    )
    return report_differences(table, expected, stored)


def load_performance(client: ServiceNowClient) -> None:
    for table in data.PERF_TABLES:
        load_records(client, table, data.perf_records(table), data.PERF_CURSOR)
    verify_performance(client)


def verify_performance(client: ServiceNowClient) -> None:
    fields = ["u_number", data.PERF_CURSOR]
    raise_differences(
        [
            error
            for table in data.PERF_TABLES
            for error in verify_records(client, table, data.perf_records(table), fields)
        ]
    )


def capture(client: ServiceNowClient) -> None:
    dictionary = client.dictionary([data.DLT_TYPES])
    fields = [
        entry["element"] for entry in dictionary if entry["element"] not in NOT_CAPTURED
    ]
    query = f"{data.SEED_ID_FIELD}STARTSWITHtypes-^ORDERBY{data.SEED_ID_FIELD}"
    page = client.get(
        f"v2/table/{data.DLT_TYPES}",
        {
            **TABLE_API_PARAMS,
            "sysparm_query": query,
            "sysparm_fields": ",".join(fields),
            "sysparm_limit": 100,
        },
    )
    export = client.export.get(
        f"{data.DLT_TYPES}_list.do?CSV",
        params={
            **CSV_EXPORT_PARAMS,
            "sysparm_query": query,
            "sysparm_fields": ",".join(fields),
            "sysparm_record_count": 100,
        },
    )
    export.raise_for_status()
    os.makedirs(data.CASES_DIR, exist_ok=True)
    with open(data.CAPTURED_DICTIONARY, "w", encoding="utf-8") as f:
        json.dump(dictionary, f, indent=2, ensure_ascii=False)
    with open(data.CAPTURED_PAGE, "w", encoding="utf-8") as f:
        json.dump(page.json(), f, indent=2, ensure_ascii=False)
    with open(data.CAPTURED_EXPORT, "wb") as f:
        f.write(export.content)
    with open(data.CAPTURED_EXPORT_HEADERS, "w", encoding="utf-8") as f:
        json.dump({"Content-Type": export.headers.get("Content-Type")}, f, indent=2)


def capture_hidden(client: ServiceNowClient) -> None:
    """Captures a Table API page and a CSV export of `u_dlt_pages` while a read ACL hides
    `data.HIDDEN_FIELD`. Two of the records have an empty `u_when`.
    """
    records = data.page_records()
    seed_ids = [
        records[i][data.SEED_ID_FIELD]
        for i in (0, 1, data.EMPTY_WHEN_RECORDS, data.EMPTY_WHEN_RECORDS + 1)
    ]
    query = f"{data.SEED_ID_FIELD}IN{','.join(seed_ids)}^ORDERBY{data.SEED_ID_FIELD}"
    fields = ",".join(data.HIDDEN_CAPTURE_FIELDS)
    page = client.get(
        f"v2/table/{data.DLT_PAGES}",
        {
            **TABLE_API_PARAMS,
            "sysparm_query": query,
            "sysparm_fields": fields,
            "sysparm_limit": 10,
        },
    )
    if any(data.HIDDEN_FIELD in record for record in page.json()["result"]):
        raise ValueError(
            f"`{data.HIDDEN_FIELD}` of `{data.DLT_PAGES}` is returned. Create the read ACL"
            " described in tests/servicenow/README.md first."
        )
    export = client.export.get(
        f"{data.DLT_PAGES}_list.do?CSV",
        params={
            **CSV_EXPORT_PARAMS,
            "sysparm_query": query,
            "sysparm_fields": fields,
            "sysparm_record_count": 10,
        },
    )
    export.raise_for_status()
    os.makedirs(data.CASES_DIR, exist_ok=True)
    with open(data.CAPTURED_HIDDEN_PAGE, "w", encoding="utf-8") as f:
        json.dump(page.json(), f, indent=2, ensure_ascii=False)
    with open(data.CAPTURED_HIDDEN_EXPORT, "wb") as f:
        f.write(export.content)


STEPS: Dict[str, Callable[[ServiceNowClient], None]] = {
    "deploy": deploy,
    "load": load,
    "capture": capture,
    "capture-hidden": capture_hidden,
}
PERF_STEPS: Dict[str, Callable[[ServiceNowClient], None]] = {
    "deploy": partial(deploy_tables, tables=PERF_TABLES),
    "load": load_performance,
    "verify": verify_performance,
}


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Seeds the ServiceNow test instance")
    parser.add_argument(
        "command",
        nargs="?",
        default="all",
        choices=sorted({"all", *STEPS, *PERF_STEPS}),
    )
    parser.add_argument(
        "--performance",
        action="store_true",
        help="deploy, load or verify the performance tables only",
    )
    args = parser.parse_args(argv)
    steps = PERF_STEPS if args.performance else STEPS
    if args.command == "all":
        commands = (
            ["deploy", "load"] if args.performance else ["deploy", "load", "capture"]
        )
    elif args.command in steps:
        commands = [args.command]
    else:
        parser.error(
            f"`{args.command}` needs {'no ' if args.performance else ''}--performance"
        )
    sn_client = ServiceNowClient(get_credentials())
    wait_for_instance(sn_client)
    if args.performance:
        with account_lock("servicenow"), record_http(
            f"seed {args.command} --performance"
        ):
            run_steps(sn_client, steps, commands)
    else:
        run_steps(sn_client, steps, commands)


def run_steps(
    client: ServiceNowClient,
    steps: Dict[str, Callable[[ServiceNowClient], None]],
    commands: List[str],
) -> None:
    for name in commands:
        steps[name](client)
        print(f"{name}: done")


if __name__ == "__main__":
    main()
