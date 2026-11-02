import json
from datetime import date, time, timezone  # noqa: I251

import dlt
from dlt.common import Decimal

from sources.servicenow import servicenow_table
from sources.servicenow.helpers.client import (
    csv_encoding,
    read_csv_table,
    read_records,
    read_string_table,
)
from sources.servicenow.helpers.convert import (
    records_to_arrow,
    records_to_rows,
    strings_to_arrow,
    unescape_formulas,
)
from sources.servicenow.helpers.credentials import BasicCredentials
from sources.servicenow.helpers.schema import dictionary_to_columns

from tests.servicenow.seed import data
from tests.servicenow.utils import (
    INCIDENT,
    INCIDENT_FIELDS,
    INCIDENT_ROW,
    TASK_FIELDS,
    FakeInstance,
    add_task_tables,
)
from tests.utils import assert_load_info, load_tables_to_dicts


def test_records_to_rows() -> None:
    columns, durations = dictionary_to_columns(
        [
            {"name": "incident", "element": element, "internal_type": type_}
            for element, type_ in {**TASK_FIELDS, **INCIDENT_FIELDS}.items()
        ],
        ["incident"],
    )
    assert records_to_rows([INCIDENT], columns, durations) == [INCIDENT_ROW]
    # empty values and fields hidden by an ACL are None, values that do not convert stay strings
    rows = records_to_rows(
        [{"sys_id": "1", "priority": "", "u_cost": "n/a"}], columns, durations
    )
    assert rows[0]["priority"] is None and rows[0]["u_due"] is None
    assert rows[0]["u_cost"] == "n/a"
    # times stored without the date
    assert records_to_rows([{"u_start": "08:00:00"}], columns, durations)[0][
        "u_start"
    ] == time(8)


def test_records_to_arrow() -> None:
    columns, durations = dictionary_to_columns(
        [
            {"name": "incident", "element": element, "internal_type": type_}
            for element, type_ in {**TASK_FIELDS, **INCIDENT_FIELDS}.items()
        ],
        ["incident"],
    )
    empty = {"sys_id": "2", **{name: "" for name in INCIDENT if name != "sys_id"}}
    table = records_to_arrow([INCIDENT, empty], columns, durations)
    assert table.column_names == list(columns)
    rows = table.to_pylist()
    assert rows[0] == INCIDENT_ROW
    assert rows[1] == {name: "2" if name == "sys_id" else None for name in columns}


def test_backends_identical(
    instance: FakeInstance, credentials: BasicCredentials
) -> None:
    empty = {
        "sys_id": "2",
        **{name: "" for name in INCIDENT if name not in ("sys_id",)},
    }
    empty["sys_updated_on"] = "2024-01-01 00:00:00"
    add_task_tables(instance, [INCIDENT, empty])
    loaded = {}
    for backend in ("python", "pyarrow"):
        pipeline = dlt.pipeline(
            pipeline_name=f"servicenow_{backend}", destination="duckdb", dev_mode=True
        )
        assert_load_info(
            pipeline.run(
                servicenow_table(
                    credentials=credentials, table="incident", backend=backend
                )
            )
        )
        columns = pipeline.default_schema.get_table("incident")["columns"]
        rows = load_tables_to_dicts(pipeline, "incident")["incident"]
        loaded[backend] = (
            # the `pyarrow` backend also stores the decimal precision of the destination
            {
                name: (c.get("data_type"), c.get("timezone"))
                for name, c in columns.items()
                if not name.startswith("_dlt")
            },
            sorted(
                (
                    {k: v for k, v in row.items() if not k.startswith("_dlt")}
                    for row in rows
                ),
                key=lambda row: row["sys_id"],
            ),
        )
    assert loaded["python"] == loaded["pyarrow"]
    types, rows = loaded["python"]
    assert types["business_duration"] == ("bigint", None)
    assert types["sys_updated_on"] == ("timestamp", True)
    assert rows[1]["business_duration"] == 187200
    assert rows[1]["u_cost"] == Decimal("1234.5678")


def test_captured_cases() -> None:
    """Reads the Table API page and CSV export captured from the seeded instance"""
    with open(data.CAPTURED_DICTIONARY, encoding="utf-8") as f:
        columns, durations = dictionary_to_columns(json.load(f), [data.DLT_TYPES])
    with open(data.CAPTURED_PAGE, "rb") as f:
        page = f.read()
    with open(data.CAPTURED_EXPORT, "rb") as f:
        export = f.read()
    with open(data.CAPTURED_EXPORT_HEADERS, encoding="utf-8") as f:
        content_type = json.load(f)["Content-Type"]
    fields = list(columns)
    encoding = csv_encoding(content_type, export)
    loaded = {
        "python": records_to_rows(read_records(page), columns, durations),
        "pyarrow": strings_to_arrow(
            read_string_table(page, fields), columns, durations
        ).to_pylist(),
        "csv": strings_to_arrow(
            unescape_formulas(read_csv_table(export, encoding, fields), columns),
            columns,
            durations,
        ).to_pylist(),
    }
    expected = {record[data.SEED_ID_FIELD]: record for record in data.RECORDS}
    for name, rows in loaded.items():
        by_seed_id = {row[data.SEED_ID_FIELD]: row for row in rows}
        assert set(by_seed_id) == set(expected), name
        for seed_id, record in expected.items():
            for field in data.FIELDS:
                if field in ("u_journal", "u_reference"):
                    continue
                value = data.expected_value(record, field)
                if name == "csv" and isinstance(value, str):
                    # the instance exports in Windows-1252
                    value = value.encode(encoding, "replace").decode(encoding)
                assert by_seed_id[seed_id][field] == value, (name, seed_id, field)
