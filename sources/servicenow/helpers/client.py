"""Reads records and table metadata of a ServiceNow instance with the Table API and CSV export"""

import codecs
import csv
import io
import threading
import time
from datetime import datetime, timezone  # noqa: I251
from email.utils import parsedate_to_datetime
from http.cookiejar import DefaultCookiePolicy
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    NoReturn,
    Optional,
    Sequence,
    Set,
    Tuple,
)
from urllib.parse import urlencode

from dlt.common import json, logger
from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import (
    ChunkedEncodingError,
    Client,
    HTTPError,
    ReadTimeout,
    Response,
    Session,
)
from dlt.sources.helpers.rest_client import RESTClient

from .credentials import ServiceNowCredentials, ServiceNowOAuth2
from .query import TableQuery, is_literal

TRecord = Dict[str, Any]

# values in the database format, date-times in UTC, references as sys_id
TABLE_API_PARAMS = {
    "sysparm_display_value": "false",
    "sysparm_exclude_reference_link": "true",
    "sysparm_no_count": "true",
    "sysparm_suppress_pagination_header": "true",
}
# raw values with field names in the header, date-times in UTC
CSV_EXPORT_PARAMS = {
    "sysparm_display_value": "false",
    "sysparm_export_column_header_label": "false",
}
# default of the `glide.export.csv.charset` system property
CSV_EXPORT_ENCODING = "cp1252"
# default of the `glide.csv.export.limit` system property, the most records an export returns
CSV_EXPORT_LIMIT = 10000
# instances reject urls longer than about 8 KB with HTTP 414
MAX_URL_LENGTH = 8000
MIN_PAGE_SIZE = 100
# larger than the 60 seconds of the default `REST Table API request timeout` quota rule
REQUEST_TIMEOUT = 300.0


class TransactionCancelled(Exception):
    """The instance cancelled a request that exceeded a transaction quota"""


def is_transaction_cancelled(text: str) -> bool:
    text = text.lower()
    return any(
        phrase in text
        for phrase in (
            "transaction cancelled",
            "transaction canceled",
            "maximum execution time",
        )
    )


def error_message(response: Response) -> str:
    """Returns the message and the detail of an error response. Returns the start of the body if
    the body is not JSON.
    """
    try:
        error = response.json().get("error") or {}
        return " ".join(
            str(error[key]) for key in ("message", "detail") if error.get(key)
        )
    except (ValueError, AttributeError):
        return response.text[:300]


def _should_retry(
    response: Optional[Response], exception: Optional[BaseException]
) -> bool:
    # a cancelled transaction is repeated with a smaller page instead
    if response is None:
        return False
    if response.status_code == 429:
        return True
    # under load the instance can answer with an empty 202 or a body without records
    if response.status_code == 202 and not response.content:
        return True
    if response.status_code == 200 and response.content.strip() in (b"null", b"[]"):
        return True
    return response.status_code >= 500 and not is_transaction_cancelled(response.text)


def _raise_for_error(error: Any) -> None:
    """Raises for the `error` object of a Table API response. The instance sends this object with
    status 200 when it cancels a request after a part of the records.
    """
    if not error:
        return
    message = (
        " ".join(str(error.get(key) or "") for key in ("message", "detail")).strip()
        if isinstance(error, dict)
        else str(error)
    )
    if is_transaction_cancelled(message):
        raise TransactionCancelled(message)
    raise ValueError(f"Table API returned an error: {message}")


def _raise_for_invalid_body(content: bytes) -> NoReturn:
    text = content.decode("utf-8", errors="replace")
    if is_transaction_cancelled(text[-2000:]):
        raise TransactionCancelled(text[-300:])
    try:
        json.loadb(content)
    except ValueError:
        # the instance can truncate the response of a cancelled transaction without a message
        if content.startswith(b'{"result":'):
            raise TransactionCancelled("Table API returned a truncated response.")
    raise ValueError(f"Table API returned an invalid response: {text[:300]}")


def read_records(content: bytes) -> List[TRecord]:
    """Returns the records of a Table API response"""
    try:
        body = json.loadb(content)
    except ValueError:
        _raise_for_invalid_body(content)
    if not isinstance(body, dict):
        _raise_for_invalid_body(content)
    _raise_for_error(body.get("error"))
    records = body.get("result")
    if not isinstance(records, list):
        _raise_for_invalid_body(content)
    return records


def read_string_table(content: bytes, fields: Sequence[str]) -> Any:
    """Reads the records of a Table API response into an Arrow table with a string column for
    each of `fields`. Fields missing from a record are nulls, other fields are skipped.
    """
    # imported here so records can be read without pyarrow installed
    from dlt.common.libs.pyarrow import pyarrow as pa
    from pyarrow import json as pa_json

    record = pa.struct([pa.field(name, pa.string()) for name in fields])
    error = pa.struct(
        [pa.field("message", pa.string()), pa.field("detail", pa.string())]
    )
    try:
        response = pa_json.read_json(
            io.BytesIO(content),
            # the response is one JSON object, it must be parsed as one block
            read_options=pa_json.ReadOptions(block_size=len(content) + 1),
            parse_options=pa_json.ParseOptions(
                explicit_schema=pa.schema(
                    [pa.field("result", pa.list_(record)), pa.field("error", error)]
                ),
                unexpected_field_behavior="ignore",
            ),
        )
    except pa.ArrowInvalid:
        _raise_for_invalid_body(content)
    if response.num_rows != 1:
        _raise_for_invalid_body(content)
    _raise_for_error(response.column("error")[0].as_py())
    result = response.column("result").combine_chunks()
    if result.null_count:
        _raise_for_invalid_body(content)
    records = result.flatten()
    return pa.Table.from_arrays(records.flatten(), names=list(fields))


def csv_encoding(content_type: str, content: bytes) -> str:
    """Returns the encoding of a CSV export:
    - the charset of `content_type`, if it has one
    - UTF-8, if `content` starts with a byte order mark or decodes as UTF-8
    - the default encoding of exports in all other cases
    """
    for param in content_type.split(";")[1:]:
        name, _, value = param.strip().partition("=")
        if name.lower() == "charset":
            return codecs.lookup(value.strip('"')).name
    # ascii is checked without decoding the page
    if content.startswith(codecs.BOM_UTF8) or content.isascii():
        return "utf-8"
    try:
        content.decode("utf-8")
    except UnicodeDecodeError:
        return CSV_EXPORT_ENCODING
    return "utf-8"


def read_csv_table(content: bytes, encoding: str, fields: Sequence[str]) -> Any:
    """Reads a CSV export into an Arrow table with a string column for each of `fields` in its
    header. Other columns are skipped. If the header has none of `fields`, the table has no
    columns. Values hidden by field ACLs are nulls.
    """
    # imported here so records can be read without pyarrow installed
    from dlt.common.libs.pyarrow import pyarrow as pa
    from pyarrow import csv as pa_csv

    # records are quoted, a cancelled export ends with a line with the message. the first and
    # the last line are sliced, the page is not copied
    end = len(content)
    while end and content[end - 1] in b" \t\r\n":
        end -= 1
    last_line = content[content.rfind(b"\n", 0, end) + 1 : end]
    if b'"' not in last_line and is_transaction_cancelled(
        last_line.decode(encoding, errors="replace")
    ):
        raise TransactionCancelled(last_line.decode(encoding, errors="replace"))
    newline = content.find(b"\n")
    first_line = content[: newline if newline >= 0 else len(content)]
    header = next(
        csv.reader([first_line.decode(encoding).lstrip("\ufeff").rstrip("\r")]), []
    )
    columns = [name for name in fields if name in header]
    if not columns:
        return pa.table({})
    return pa_csv.read_csv(
        # a native reader does not call python to read, pages are parsed in parallel
        pa.BufferReader(content),
        read_options=pa_csv.ReadOptions(encoding=encoding),
        parse_options=pa_csv.ParseOptions(newlines_in_values=True),
        convert_options=pa_csv.ConvertOptions(
            column_types={name: pa.string() for name in columns},
            # exports quote empty values, values hidden by field ACLs are empty and not quoted
            null_values=[""],
            strings_can_be_null=True,
            quoted_strings_can_be_null=False,
            include_columns=columns,
        ),
    )


def _selected_fields(fields: Optional[Sequence[str]], query: TableQuery) -> List[str]:
    """Returns `fields` with the keys of `query`, empty if `fields` are not set"""
    if not fields:
        return []
    return [*fields, *(key for key in query.keys if key not in fields)]


def page_keys(page: Any, keys: Sequence[str]) -> List[Tuple[Optional[str], ...]]:
    """Returns a tuple of `keys` values for each record of `page`, a list of records or an Arrow
    table. A value missing from a record is None.
    """
    if isinstance(page, list):
        return [tuple(record.get(key) for key in keys) for record in page]
    return list(zip(*(page.column(key).to_pylist() for key in keys)))


def fit_url(table: str, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Returns `params` without `sysparm_fields` if the url with `sysparm_fields` is too long. The
    instance then returns all fields. Raises if the url without `sysparm_fields` is too long.
    """
    if len(url) + len(urlencode(params)) <= MAX_URL_LENGTH:
        return params
    params = {key: value for key, value in params.items() if key != "sysparm_fields"}
    length = len(url) + len(urlencode(params))
    if length > MAX_URL_LENGTH:
        raise ValueError(
            f"The url for the query of `{table}` has {length} characters, but the instance"
            f" accepts about {MAX_URL_LENGTH}. Shorten the encoded query."
        )
    return params


def make_session() -> Session:
    """Returns a session that does not keep cookies. The session retries HTTP 429, empty
    responses, 5xx and connection errors, but not the 5xx of a cancelled transaction and not read
    timeouts. Retries obey the `Retry-After` header.
    """
    session = Client(
        request_timeout=REQUEST_TIMEOUT,
        raise_for_status=False,
        status_codes=(),
        # a page that times out is read again with half of the records
        exceptions=(requests.ConnectionError, ChunkedEncodingError),
        retry_condition=_should_retry,
    ).session
    # the instance runs the requests that share a session cookie one at a time. without the
    # cookie, tables are read in parallel
    session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))
    return session


class ServiceNowClient:
    """Reads tables of a ServiceNow instance. Threads can share one client."""

    def __init__(
        self, credentials: ServiceNowCredentials, session: Optional[Session] = None
    ) -> None:
        session = session or make_session()
        self.base_url = credentials.base_url
        self.auth = credentials.make_auth(session)
        self.rest = RESTClient(
            base_url=f"{self.base_url}/api/now/",
            headers={"Accept": "application/json"},
            auth=self.auth,
            session=session,
        )
        self.export = RESTClient(
            base_url=self.base_url, auth=self.auth, session=session
        )
        self._tables: Optional[Dict[str, str]] = None
        self._tables_lock = threading.Lock()

    def get(self, path: str, params: Dict[str, Any]) -> Response:
        """Sends a GET request to `path` under `/api/now/`. Raises on HTTP errors, HTML pages and
        empty responses. Sends the request again one time after HTTP 401.
        """
        response = self.rest.get(path, params=params)
        if response.status_code == 401:
            # the instance can revoke a token before it expires and can reject valid credentials
            # once
            if isinstance(self.auth, ServiceNowOAuth2):
                self.auth.access_token = None
            response = self.rest.get(path, params=params)
        if response.status_code >= 400:
            message = (
                f"The instance returned HTTP {response.status_code} for `{path}`:"
                f" {error_message(response)}"
            )
            if response.status_code == 403 and "present in the query" in message:
                # query ACLs deny conditions with `=` or ISEMPTY (match) and `>` or `<` (range)
                message += (
                    ". Grant `query_match` and `query_range` access to the fields of the query"
                    f" `{params.get('sysparm_query')}`."
                )
            raise HTTPError(message, response=response)
        if response.headers.get("Content-Type", "").startswith("text/html"):
            # login and hibernation pages are returned with status 200
            raise ValueError(
                f"Instance `{self.base_url}` returned an HTML page instead of JSON for `{path}`."
                " Make sure that `instance` is the name or url of the instance. If it is a"
                " developer instance in hibernation, wake it."
            )
        if not response.content:
            raise ValueError(
                f"Instance `{self.base_url}` returned an empty response with status"
                f" {response.status_code} for `{path}`"
            )
        return response

    def server_time(self, table: str) -> datetime:
        """Returns the time of the instance from the `Date` header of a request to `table`. Raises
        if the request fails.
        """
        response = self.get(
            f"v2/table/{table}",
            {**TABLE_API_PARAMS, "sysparm_limit": 1, "sysparm_fields": "sys_id"},
        )
        date = response.headers.get("Date")
        if not date:
            logger.warning(
                f"The response for `{table}` had no `Date` header. dlt uses the local time"
                " instead."
            )
            return datetime.now(timezone.utc)
        return parsedate_to_datetime(date)

    def count(self, table: str, query: str) -> Optional[int]:
        """Returns the number of records of `table` that match `query`, before the instance
        applies ACLs. Returns None if the instance does not count them, for example for remote
        tables.
        """
        params = {
            key: value
            for key, value in TABLE_API_PARAMS.items()
            if key != "sysparm_no_count"
        }
        response = self.get(
            f"v2/table/{table}",
            {
                **params,
                "sysparm_query": query,
                "sysparm_limit": 1,
                "sysparm_fields": "sys_id",
            },
        )
        total = response.headers.get("X-Total-Count")
        return int(total) if total else None

    def pages(
        self,
        table: str,
        query: TableQuery,
        fields: Optional[Sequence[str]] = None,
        page_size: int = 10000,
    ) -> Iterator[List[TRecord]]:
        """Yields the pages of records of `table` that match `query`. A page has `fields`, or all
        fields if `fields` is not set.

        Records come in the order of the query keys. A page has at most `page_size` records. The
        instance applies ACLs after the limit, so a page before the last can be short or empty.
        After an empty page, reading continues with an offset. It stops when the offset is at
        least the number of remaining records, counted before ACLs. A page cancelled by a
        transaction quota or a timeout is read again with half of the records.
        """
        return self._keyset_pages(
            table,
            query,
            page_size,
            self._table_api_fetcher(table, query, fields, False),
        )

    def arrow_pages(
        self,
        table: str,
        query: TableQuery,
        fields: Sequence[str],
        page_size: int = 10000,
    ) -> Iterator[Any]:
        """Same as `pages`, yields Arrow tables with a string column for each of `fields` and
        the query keys. Requires pyarrow.
        """
        return self._keyset_pages(
            table, query, page_size, self._table_api_fetcher(table, query, fields, True)
        )

    def csv_pages(
        self,
        table: str,
        query: TableQuery,
        fields: Sequence[str],
        page_size: int = 10000,
    ) -> Iterator[Any]:
        """Same as `arrow_pages`, reads pages of the CSV export of `table`. Requires pyarrow.

        Pages are limited by the `glide.csv.export.limit` system property, 10000 by default.
        Pages read with an offset use the Table API: the pages after an empty page and after keys
        that are not literals.
        """
        selected = _selected_fields(fields, query)
        table_api_page = self._table_api_fetcher(table, query, fields, True)
        encodings: List[str] = []

        def fetch_page(page_query: str, limit: int, offset: int) -> Any:
            if offset:
                return table_api_page(page_query, limit, offset)
            path = f"{table}_list.do?CSV"
            params = fit_url(
                table,
                f"{self.base_url}/{path}",
                {
                    **CSV_EXPORT_PARAMS,
                    "sysparm_query": page_query,
                    "sysparm_record_count": limit,
                    "sysparm_fields": ",".join(selected),
                },
            )
            if "sysparm_fields" not in params:
                params["sysparm_default_export_fields"] = "all"
            response = self.export.get(path, params=params)
            if response.status_code == 401:
                raise ValueError(
                    f"The instance did not authorize the CSV export of `{table}`. Exports"
                    " accept basic authentication and OAuth tokens. Basic authentication can"
                    " require the `snc_basic_auth_api_access` role."
                )
            if response.status_code >= 400:
                raise HTTPError(
                    f"The instance returned HTTP {response.status_code} for the CSV export of"
                    f" `{table}`:"
                    f" {response.text[:300]}",
                    response=response,
                )
            if response.headers.get("Content-Type", "").startswith("text/html"):
                if is_transaction_cancelled(response.text):
                    raise TransactionCancelled(response.text[:300])
                # the login page is returned for credentials that the instance does not accept
                raise ValueError(
                    f"The CSV export of `{table}` returned an HTML page instead of CSV. The"
                    " instance can reject these credentials for exports. Use basic authentication"
                    " or `api='table'`."
                )
            content = response.content.removeprefix(codecs.BOM_UTF8)
            if not content or content.isspace():
                return []
            encoding = csv_encoding(
                response.headers.get("Content-Type", ""), response.content
            )
            if encoding != "utf-8" and encoding not in encodings:
                encodings.append(encoding)
                logger.warning(
                    f"The instance exported CSV in `{encoding}`. Characters of `{table}` without"
                    f" an `{encoding}` code are lost. Set the `glide.export.csv.charset` system"
                    " property to UTF-8."
                )
            page = read_csv_table(response.content, encoding, selected)
            if not all(key in page.column_names for key in query.keys):
                raise ValueError(
                    f"CSV export of `{table}` has no {query.keys} columns. Its header is"
                    f" {page.column_names}."
                )
            return page

        return self._keyset_pages(table, query, page_size, fetch_page)

    def _table_api_fetcher(
        self,
        table: str,
        query: TableQuery,
        fields: Optional[Sequence[str]],
        as_arrow: bool,
    ) -> Callable[[str, int, int], Any]:
        """Returns a function that reads a Table API page for a query, a limit and an offset"""
        selected = _selected_fields(fields, query)
        path = f"v2/table/{table}"

        def fetch_page(page_query: str, limit: int, offset: int) -> Any:
            page_params: Dict[str, Any] = {
                **TABLE_API_PARAMS,
                "sysparm_query": page_query,
                "sysparm_limit": limit,
            }
            if offset:
                page_params["sysparm_offset"] = offset
            if selected:
                page_params["sysparm_fields"] = ",".join(selected)
            page_params = fit_url(table, f"{self.base_url}/api/now/{path}", page_params)
            content = self.get(path, page_params).content
            if as_arrow:
                return read_string_table(content, selected)
            return read_records(content)

        return fetch_page

    def _keyset_pages(
        self,
        table: str,
        query: TableQuery,
        page_size: int,
        fetch_page: Callable[[str, int, int], Any],
    ) -> Iterator[Any]:
        """Yields pages returned by `fetch_page` for the query of each page, the page size and the
        number of records to skip
        """
        after: Optional[List[str]] = None
        previous_keys: Set[Tuple[str, ...]] = set()
        offset = 0
        remaining: Optional[int] = None
        while True:
            page_query = query.page_query(after)
            started = time.monotonic()
            try:
                page = fetch_page(page_query, page_size, offset)
            except (HTTPError, TransactionCancelled, ReadTimeout) as ex:
                cancelled = not isinstance(ex, HTTPError) or (
                    ex.response is not None
                    and is_transaction_cancelled(ex.response.text)
                )
                if page_size <= MIN_PAGE_SIZE or not cancelled:
                    raise
                page_size = max(page_size // 2, MIN_PAGE_SIZE)
                logger.warning(
                    f"A request for `{table}` exceeded a transaction quota or the request"
                    f" timeout. dlt now reads {page_size} records per page."
                )
                continue
            if not len(page):
                # ACLs can hide all records of a page. the offset skips them while records remain
                if remaining is None:
                    remaining = self.count(table, page_query)
                    if remaining is None:
                        logger.warning(
                            f"The instance does not count records of `{table}` after {after}."
                            " dlt stopped at an empty page and did not read the records after"
                            " it."
                        )
                        return
                offset += page_size
                if offset >= remaining:
                    return
                if offset == page_size:
                    logger.warning(
                        f"The instance counts {remaining} records of `{table}` after {after},"
                        " but ACLs hide a page of them. dlt skipped this page. Records that ACLs"
                        " hide make reading slow. Grant read access to all records of the table."
                    )
                continue
            keys = page_keys(page, query.keys)
            if any(None in key for key in keys):
                raise ValueError(
                    f"Records of `{table}` are returned without {query.keys}, for example"
                    " because of field ACLs. dlt cannot read pages ordered by these fields. Grant"
                    " read access to these fields."
                )
            if previous_keys.intersection(keys):
                # the instance ignores conditions that it cannot parse
                raise ValueError(
                    f"Records of `{table}` are not ordered by {query.keys}: the instance ignored"
                    " a part of the query, and dlt would read the same records again."
                )
            logger.info(
                f"Read {len(page)} records of `{table}` in {time.monotonic() - started:.1f}s"
            )
            previous_keys = set(keys)
            last = [str(value) for value in keys[-1]]
            if all(is_literal(value) for value in last):
                after, offset = last, 0
            else:
                # the next page uses the same keys and an offset that skips the records already
                # read
                offset += page_size
            remaining = None
            yield page

    def records(
        self, table: str, query: TableQuery, fields: Optional[Sequence[str]] = None
    ) -> Iterator[TRecord]:
        for page in self.pages(table, query, fields):
            yield from page

    def tables(self) -> Dict[str, str]:
        """Returns names of all tables with the name of their parent table, empty for base tables"""
        if self._tables is None:
            with self._tables_lock:
                if self._tables is None:
                    fields = ["name", "super_class.name"]
                    records = list(self.records("sys_db_object", TableQuery(), fields))
                    if any(name not in record for record in records for name in fields):
                        raise ValueError(
                            f"Records of `sys_db_object` are returned without {fields}, for"
                            " example because of field ACLs. Grant read access to these fields."
                        )
                    self._tables = {
                        record["name"]: record["super_class.name"] for record in records
                    }
        return self._tables

    def dictionary(self, tables: Sequence[str]) -> List[TRecord]:
        """Returns the `sys_dictionary` fields of `tables`"""
        query = TableQuery([f"nameIN{','.join(tables)}", "elementISNOTEMPTY"])
        return list(
            self.records("sys_dictionary", query, ["name", "element", "internal_type"])
        )
