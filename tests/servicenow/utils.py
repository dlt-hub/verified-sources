import hashlib
import json
import re
import threading
from datetime import date, datetime, time, timezone  # noqa: I251
from email.utils import format_datetime
from time import monotonic, sleep
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import parse_qs, urlparse

import dlt
import requests_mock
from dlt.common import Decimal
from dlt.common.configuration import with_config

from sources.servicenow.helpers.client import ServiceNowClient
from sources.servicenow.helpers.credentials import ServiceNowAuth, ServiceNowCredentials

INSTANCE = "dev1"
BASE_URL = "https://dev1.service-now.com"

TRecord = Dict[str, str]

# exports put an apostrophe before text that starts with a formula character. Numbers and
# dates are not text
NOT_TEXT_TYPES = {
    "boolean",
    "integer",
    "longint",
    "decimal",
    "float",
    "currency",
    "price",
    "glide_date_time",
    "glide_date",
    "glide_time",
    "glide_duration",
    "due_date",
}

_CONDITION = re.compile(
    r"^([a-z][a-z0-9_.]*?)(ISNOTEMPTY|ISEMPTY|IN|>=|<=|!=|>|<|=)(.*)$"
)


def _csv_line(values: List[Optional[str]]) -> str:
    # exports quote all values, values hidden by field ACLs are empty and not quoted
    return (
        ",".join(
            "" if value is None else '"' + value.replace('"', '""') + '"'
            for value in values
        )
        + "\r\n"
    )


# internal types that the instance compares and sorts as numbers
NUMERIC_TYPES = {"integer", "longint", "decimal", "currency", "price", "float"}


def _sort_key(value: str, numeric: bool) -> Any:
    # empty values come first in ascending order
    return (value != "", Decimal(value) if numeric and value else value)


def _matches(
    record: TRecord,
    condition: str,
    collation: Callable[[str], str] = str,
    numeric_fields: Set[str] = frozenset(),  # type: ignore[assignment]
) -> bool:
    match = _CONDITION.match(condition)
    assert match, f"unsupported condition `{condition}`"
    field, op, value = match.groups()
    actual: Any = collation(record.get(field, ""))
    value = collation(value)
    if op == "ISNOTEMPTY":
        return actual != ""
    # like on an instance, a comparison with an empty literal selects empty values and a
    # comparison with a value does not
    if op == "ISEMPTY" or (value == "" and op != "!="):
        return actual == ""
    if actual == "" and op != "!=":
        return False
    if op == "IN":
        return actual in value.split(",")
    if field in numeric_fields:
        actual, value = Decimal(actual), Decimal(value)
    return {
        ">=": actual >= value,
        "<=": actual <= value,
        "!=": actual != value,
        ">": actual > value,
        "<": actual < value,
        "=": actual == value,
    }[op]


def evaluate_query(
    records: List[TRecord],
    query: str,
    collation: Callable[[str], str] = str,
    numeric_fields: Set[str] = frozenset(),  # type: ignore[assignment]
) -> List[TRecord]:
    """Filters and orders `records` by an encoded query with AND, OR, NQ, ORDERBY and ORDERBYDESC
    parts. Values are compared with `collation` applied, `numeric_fields` as numbers. Empty values
    sort first in ascending order and last in descending order.
    """
    # blocks of conditions, each condition is a list of alternatives joined by OR
    blocks: List[List[List[str]]] = [[]]
    order_by: List[Tuple[str, bool]] = []
    for token in query.split("^") if query else []:
        if token.startswith("NQ"):
            blocks.append([[token[2:]]])
        elif token.startswith("ORDERBYDESC"):
            order_by.append((token[len("ORDERBYDESC") :], True))
        elif token.startswith("ORDERBY"):
            order_by.append((token[len("ORDERBY") :], False))
        elif token.startswith("OR"):
            blocks[-1][-1].append(token[2:])
        else:
            blocks[-1].append([token])
    selected = [
        record
        for record in records
        if any(
            all(
                any(
                    _matches(record, alternative, collation, numeric_fields)
                    for alternative in condition
                )
                for condition in block
            )
            for block in blocks
        )
    ]
    # stable sorts from the last key to the first
    for key, descending in reversed(order_by):
        selected.sort(
            key=lambda record: _sort_key(
                collation(record.get(key, "")), key in numeric_fields
            ),
            reverse=descending,
        )
    return selected


class FakeInstance:
    """ServiceNow instance that answers Table API v2 and CSV export requests from in-memory tables.

    Applies `sysparm_query`, `sysparm_limit`, `sysparm_offset` and `sysparm_fields`. Sends
    `X-Total-Count` without `sysparm_no_count`, except for tables in `uncounted`.

    Like ACLs, removes records whose `sys_id` is in `hidden` after it applies the limit and counts
    the records. Like field ACLs, removes `hidden_fields` from records and exports them empty. Like
    query ACLs, answers queries with conditions on `denied_fields` with a 403 from the Table API and
    no records from exports. Tables in `ignore_keyset` drop `sys_id>` conditions, and fields in
    `ignored_fields` drop their conditions. Values are compared and ordered with `collation`
    applied.

    `failures` maps a table to the responses (status code, error message, headers) to its next
    requests, or to the exceptions they raise, like timeouts. A status 200 sends the first half of
    the page with the error, like a cancelled transaction. CSV exports are encoded with `csv_encoding` and sent with `csv_content_type`.
    They have field labels in the header when `csv_header_labels` is set.
    """

    def __init__(
        self, m: requests_mock.Mocker, now: str = "2024-03-01 00:00:00"
    ) -> None:
        self.tables: Dict[str, List[TRecord]] = {}
        self.field_types: Dict[str, Dict[str, str]] = {}
        self.hidden: Set[str] = set()
        self.hidden_fields: Set[str] = set()
        # names of the threads that sent requests
        self.threads: Set[str] = set()
        self.denied_fields: Set[str] = set()
        self.uncounted: Set[str] = set()
        self.ignore_keyset: Set[str] = set()
        self.ignored_fields: Set[str] = set()
        self.collation: Callable[[str], str] = str
        self.failures: Dict[
            str, List[Union[Tuple[int, str, Dict[str, str]], Exception]]
        ] = {}
        self.requests: List[Tuple[str, Dict[str, str], Dict[str, str]]] = []
        self.token_requests: List[Dict[str, List[str]]] = []
        self.token_expires_in = 1799
        self.now = now
        self.csv_encoding = "utf-8"
        self.csv_content_type = "text/csv;charset=UTF-8"
        self.csv_header_labels = False
        m.get(re.compile(rf"{BASE_URL}/api/now/v2/table/\w+"), content=self._table_api)
        m.get(re.compile(rf"{BASE_URL}/\w+_list\.do"), content=self._csv_export)
        m.post(f"{BASE_URL}/oauth_token.do", json=self._token)

    def add_table(
        self,
        name: str,
        fields: Dict[str, str],
        parent: str = "",
        records: Optional[List[TRecord]] = None,
    ) -> None:
        """Adds table `name` to `sys_db_object` and its `fields` with their internal types to
        `sys_dictionary`
        """
        self.tables.setdefault("sys_db_object", []).append(
            {
                "sys_id": f"{len(self.tables['sys_db_object']):032x}",
                "name": name,
                "super_class.name": parent,
            }
        )
        dictionary = self.tables.setdefault("sys_dictionary", [])
        for element, internal_type in fields.items():
            dictionary.append(
                {
                    "sys_id": f"{len(dictionary):032x}",
                    "name": name,
                    "element": element,
                    "internal_type": internal_type,
                }
            )
        self.field_types[name] = {**self.field_types.get(parent, {}), **fields}
        self.tables[name] = records or []

    def queries(self, table: str) -> List[str]:
        """Returns queries of the pages of `table`"""
        return [
            params.get("sysparm_query", "")
            for name, params, _ in self.requests
            if name == table and not self._is_count(params)
        ]

    def page_requests(self, table: str) -> List[Tuple[Dict[str, str], Dict[str, str]]]:
        """Returns params and headers of the requests of pages of `table`"""
        return [
            (params, headers)
            for name, params, headers in self.requests
            if name == table and not self._is_count(params)
        ]

    def count_queries(self, table: str) -> List[str]:
        """Returns queries of the record counts of `table`"""
        return [
            params.get("sysparm_query", "")
            for name, params, _ in self.requests
            if name == table and self._is_count(params)
        ]

    @staticmethod
    def _is_count(params: Dict[str, str]) -> bool:
        return "CSV" not in params and params.get("sysparm_no_count") != "true"

    def _request(
        self, table: str, request: Any, context: Any
    ) -> Tuple[Dict[str, str], Optional[str]]:
        """Records the request and returns its params and the error message of a failure"""
        # requests_mock lowercases `request.qs`, values are case sensitive
        params = {
            key: values[0]
            for key, values in parse_qs(
                urlparse(request.url).query, keep_blank_values=True
            ).items()
        }
        self.requests.append((table, params, dict(request.headers)))
        self.threads.add(threading.current_thread().name)
        context.headers["Date"] = format_datetime(
            datetime.fromisoformat(self.now).replace(tzinfo=timezone.utc), usegmt=True
        )
        if self.failures.get(table):
            failure = self.failures[table].pop(0)
            if isinstance(failure, Exception):
                raise failure
            context.status_code, message, headers = failure
            context.headers.update(headers)
            return params, message
        if table not in self.tables:
            context.status_code = 403
            return params, "User Not Authorized"
        return params, None

    def _denied(self, params: Dict[str, str]) -> bool:
        return any(
            re.match(rf"^(OR|NQ)?{field}([<>=!]|IS)", token)
            for token in params.get("sysparm_query", "").split("^")
            for field in self.denied_fields
        )

    def _page(
        self, table: str, params: Dict[str, str], limit: int
    ) -> Tuple[List[TRecord], int]:
        """Returns the records of the page requested with `params` and the number of records
        matching the query before ACLs
        """
        tokens = params.get("sysparm_query", "").split("^")
        if table in self.ignore_keyset:
            tokens = [token for token in tokens if "sys_id>" not in token]
        tokens = [
            token
            for token in tokens
            if not any(
                re.match(rf"^(OR)?{field}([<>=!]|IS)", token)
                for field in self.ignored_fields
            )
        ]
        numeric_fields = {
            field
            for field, type_ in self.field_types.get(table, {}).items()
            if type_ in NUMERIC_TYPES
        }
        matching = evaluate_query(
            self.tables[table],
            "^".join(filter(None, tokens)),
            self.collation,
            numeric_fields,
        )
        offset = int(params.get("sysparm_offset", 0))
        records = [
            r
            for r in matching[offset : offset + limit]
            if r.get("sys_id") not in self.hidden
        ]
        return records, len(matching)

    def _table_api(self, request: Any, context: Any) -> bytes:
        context.headers["Content-Type"] = "application/json;charset=UTF-8"
        body = self._table_api_body(request, context)
        return b"" if body is None else json.dumps(body).encode()

    def _table_api_body(self, request: Any, context: Any) -> Any:
        table = urlparse(request.url).path.rsplit("/", 1)[1]
        params, error = self._request(table, request, context)
        if context.status_code == 202:
            return None
        if error and context.status_code != 200:
            return {"error": {"message": error, "detail": None}, "status": "failure"}
        if self._denied(params):
            context.status_code = 403
            return {
                "error": {
                    "message": "Insufficient rights to query records",
                    "detail": "Field(s) present in the query do not have permission to be read",
                },
                "status": "failure",
            }
        records, count = self._page(table, params, int(params["sysparm_limit"]))
        if params.get("sysparm_no_count") != "true" and table not in self.uncounted:
            context.headers["X-Total-Count"] = str(count)
        names = (params.get("sysparm_fields") or "").split(",")
        records = [
            {
                key: value
                for key, value in r.items()
                if key not in self.hidden_fields and (names == [""] or key in names)
            }
            for r in records
        ]
        if error:
            return {
                "result": records[: len(records) // 2],
                "error": {"message": error, "detail": error},
                "status": "failure",
            }
        return {"result": records}

    def _csv_export(self, request: Any, context: Any) -> bytes:
        table = urlparse(request.url).path.rsplit("/", 1)[1][: -len("_list.do")]
        params, error = self._request(table, request, context)
        assert "CSV" in params
        if error and context.status_code != 200:
            return error.encode()
        context.headers["Content-Type"] = self.csv_content_type
        if self._denied(params):
            return b""
        records, _ = self._page(table, params, int(params["sysparm_record_count"]))
        if params.get("sysparm_default_export_fields") == "all":
            names = list(dict.fromkeys(key for r in self.tables[table] for key in r))
        else:
            names = params["sysparm_fields"].split(",")
        if not records:
            return b""
        header: List[Optional[str]] = [
            name.replace("_", " ").title() if self.csv_header_labels else name
            for name in names
        ]
        types = self.field_types.get(table, {})
        rows = [
            [
                (
                    None
                    if name in self.hidden_fields
                    else self._export_value(r.get(name, ""), types.get(name))
                )
                for name in names
            ]
            for r in records
        ]
        content = "".join(
            _csv_line(row)
            for row in [header, *(rows[: len(rows) // 2] if error else rows)]
        )
        if error:
            content += error
        return content.encode(self.csv_encoding, errors="replace")

    @staticmethod
    def _export_value(value: str, internal_type: Optional[str]) -> str:
        if internal_type == "boolean":
            return {"true": "1", "false": "0"}.get(value, value)
        if internal_type not in NOT_TEXT_TYPES and value[:1] in ("=", "+", "-", "@"):
            return "'" + value
        return value

    def _token(self, request: Any, context: Any) -> Dict[str, Any]:
        self.token_requests.append(parse_qs(request.text))
        return {
            "access_token": f"token-{len(self.token_requests)}",
            "token_type": "Bearer",
            "expires_in": self.token_expires_in,
        }


def make_records(
    count: int, updated_on: Callable[[int], str], **values: str
) -> List[TRecord]:
    """Returns `count` records with hex `sys_id` in reverse creation order and `values`"""
    return [
        {
            "sys_id": f"{count - i:032x}",
            "sys_updated_on": updated_on(i),
            "sys_created_on": "2024-01-01 00:00:00",
            **values,
        }
        for i in range(count)
    ]


TASK_FIELDS = {
    "sys_id": "GUID",
    "number": "string",
    "active": "boolean",
    "priority": "integer",
    "sys_updated_on": "glide_date_time",
    "sys_created_on": "glide_date_time",
    "sys_class_name": "sys_class_name",
    "work_notes": "journal_input",
    "business_duration": "glide_duration",
}

INCIDENT_FIELDS = {
    "caller_id": "reference",
    "u_cost": "currency",
    "u_score": "float",
    "u_due": "glide_date",
    "u_start": "glide_time",
    # replaces the inherited field
    "number": "integer",
}

INCIDENT: TRecord = {
    "sys_id": "46d44a23a9fe19810012d100cca80666",
    "number": "10001",
    "active": "true",
    "priority": "1",
    "sys_updated_on": "2024-02-01 10:30:00",
    "sys_created_on": "2024-01-15 08:00:00",
    "sys_class_name": "incident",
    "work_notes": "",
    "business_duration": "1970-01-03 04:00:00",
    "caller_id": "5137153cc611227c000bbd1bd8cd2005",
    "u_cost": "1234.5678",
    "u_score": "0.1",
    "u_due": "2024-02-29",
    "u_start": "1970-01-01 12:34:56",
}

INCIDENT_ROW: Dict[str, Any] = {
    "sys_id": "46d44a23a9fe19810012d100cca80666",
    "number": 10001,
    "active": True,
    "priority": 1,
    "sys_updated_on": datetime(2024, 2, 1, 10, 30, tzinfo=timezone.utc),
    "sys_created_on": datetime(2024, 1, 15, 8, tzinfo=timezone.utc),
    "sys_class_name": "incident",
    "business_duration": 187200,
    "caller_id": "5137153cc611227c000bbd1bd8cd2005",
    "u_cost": Decimal("1234.5678"),
    "u_score": 0.1,
    "u_due": date(2024, 2, 29),
    "u_start": time(12, 34, 56),
}


def add_task_tables(
    instance: FakeInstance, incidents: Optional[List[TRecord]] = None
) -> None:
    instance.add_table("task", TASK_FIELDS)
    instance.add_table("incident", INCIDENT_FIELDS, parent="task", records=incidents)


def updated_on(i: int) -> str:
    return f"2024-02-{i // 24 + 1:02d} {i % 24:02d}:00:00"


def random_sys_id(i: int) -> str:
    return hashlib.md5(str(i).encode()).hexdigest()


def random_id_records(count: int, **values: str) -> List[TRecord]:
    """Returns records with random hex `sys_id`s, like the ones the instance generates"""
    records = make_records(count, updated_on, **values)
    for i, record in enumerate(records):
        record["sys_id"] = random_sys_id(i)
    return records


def item_rows(items: List[Any]) -> List[Dict[str, Any]]:
    """Returns rows of items yielded by a resource, dicts or Arrow tables"""
    return [
        row
        for item in items
        for row in (item.to_pylist() if hasattr(item, "to_pylist") else [item])
    ]


TRANSACTION_CANCELLED = "Transaction cancelled: maximum execution time exceeded"


@with_config(sections=("sources", "servicenow"))
def get_credentials(
    credentials: ServiceNowAuth = dlt.secrets.value,
) -> ServiceNowCredentials:
    """Returns the credentials of the test instance from the dlt config"""
    return credentials


def wait_for_instance(
    client: ServiceNowClient, timeout: float = 300, interval: float = 15
) -> None:
    """Waits until a developer instance that wakes up from hibernation answers with JSON instead of
    an HTML page. Other errors raise at once.
    """
    deadline = monotonic() + timeout
    while True:
        try:
            client.get(
                "v2/table/sys_db_object",
                {"sysparm_limit": 1, "sysparm_fields": "sys_id"},
            )
            return
        except ValueError as ex:
            if "HTML page" not in str(ex) or monotonic() > deadline:
                raise
        sleep(interval)
