from typing import List

import pendulum
from dlt.common import Decimal
from dlt.common.data_types.typing import DATA_TYPES
from dlt.common.schema import TColumnSchema, TTableSchema, TTableSchemaColumns
from dlt.common.typing import TDataItem

TABLE_ROW_ALL_DATA_TYPES = {
    "col1": 989127831,
    "col2": 898912.821982,
    "col3": True,
    "col4": "2022-05-23T13:26:45.176451+00:00",
    "col5": "string data \n \r \x8e 🦆",
    "col6": Decimal("2323.34"),
    "col7": b"binary data \n \r \x8e",
    # "col8": 2**56 + 92093890840,  # TODO: uncommment and make it work
    "col9": {
        "complex": [1, 2, 3, "a"],
        "link": (
            "?commen\ntU\nrn=urn%3Ali%3Acomment%3A%28acti\012 \6"
            " \\vity%3A69'08444473\n\n551163392%2C6n \r \x8e9085"
        ),
    },
    "col10": "2023-02-27",
    "col11": "13:26:45.176451",
    "col1_null": None,
    "col2_null": None,
    "col3_null": None,
    "col4_null": None,
    "col5_null": None,
    "col6_null": None,
    "col7_null": None,
    # "col8_null": None,
    "col9_null": None,
    "col10_null": None,
    "col11_null": None,
    "col1_precision": 22324,
    "col4_precision": "2022-05-23T13:26:46.167231+00:00",
    "col5_precision": "string data 2 \n \r \x8e 🦆",
    "col6_precision": Decimal("2323.34"),
    "col7_precision": b"binary data 2 \n \r \x8e",
    "col11_precision": "13:26:45.176451",
}
TABLE_UPDATE: List[TColumnSchema] = [
    {"name": "col1", "data_type": "bigint", "nullable": False},
    {"name": "col2", "data_type": "double", "nullable": False},
    {"name": "col3", "data_type": "bool", "nullable": False},
    {"name": "col4", "data_type": "timestamp", "nullable": False},
    {"name": "col5", "data_type": "text", "nullable": False},
    {"name": "col6", "data_type": "decimal", "nullable": False},
    {"name": "col7", "data_type": "binary", "nullable": False},
    # {"name": "col8", "data_type": "wei", "nullable": False},
    {"name": "col9", "data_type": "json", "nullable": False, "variant": True},  # type: ignore[typeddict-item]
    {"name": "col10", "data_type": "date", "nullable": False},
    {"name": "col11", "data_type": "time", "nullable": False},
    {"name": "col1_null", "data_type": "bigint", "nullable": True},
    {"name": "col2_null", "data_type": "double", "nullable": True},
    {"name": "col3_null", "data_type": "bool", "nullable": True},
    {"name": "col4_null", "data_type": "timestamp", "nullable": True},
    {"name": "col5_null", "data_type": "text", "nullable": True},
    {"name": "col6_null", "data_type": "decimal", "nullable": True},
    {"name": "col7_null", "data_type": "binary", "nullable": True},
    # {"name": "col8_null", "data_type": "wei", "nullable": True},
    {"name": "col9_null", "data_type": "json", "nullable": True, "variant": True},  # type: ignore[typeddict-item]
    {"name": "col10_null", "data_type": "date", "nullable": True},
    {"name": "col11_null", "data_type": "time", "nullable": True},
    {
        "name": "col1_precision",
        "data_type": "bigint",
        "precision": 16,
        "nullable": False,
    },
    {
        "name": "col4_precision",
        "data_type": "timestamp",
        "precision": 3,
        "nullable": False,
    },
    {"name": "col5_precision", "data_type": "text", "precision": 25, "nullable": False},
    {
        "name": "col6_precision",
        "data_type": "decimal",
        "precision": 6,
        "scale": 2,
        "nullable": False,
    },
    {
        "name": "col7_precision",
        "data_type": "binary",
        "precision": 19,
        "nullable": False,
    },
    {"name": "col11_precision", "data_type": "time", "precision": 3, "nullable": False},
]

if "complex" in DATA_TYPES:
    for col_schema in TABLE_UPDATE:
        if col_schema["data_type"] == "json":
            col_schema["data_type"] = "complex"

TABLE_UPDATE_COLUMNS_SCHEMA: TTableSchemaColumns = {t["name"]: t for t in TABLE_UPDATE}

ROW_MESSAGES: List[dict] = [
    {
        "transactionId": 969,
        "commitTime": "1728662646949062",
        "table": "src_pl_dataset_202410110404048747_staging.tbl_y",
        "op": "INSERT",
        "newTuple": [
            {
                "columnName": "id_y",
                "columnType": "20",
                "datumInt64": "2",
            },
            {
                "columnName": "val_y",
                "columnType": "16",
                "datumBool": False,
            },
            {
                "columnName": "_dlt_load_id",
                "columnType": "1043",
                "datumString": "1728662646.2657657",
            },
            {
                "columnName": "_dlt_id",
                "columnType": "1043",
                "datumString": "gGjifTMTAUs5ag",
            },
        ],
        "newTypeinfo": [
            {
                "modifier": "bigint",
                "valueOptional": False,
            },
            {
                "modifier": "boolean",
                "valueOptional": True,
            },
            {
                "modifier": "character varying",
                "valueOptional": False,
            },
            {
                "modifier": "character varying",
                "valueOptional": False,
            },
        ],
        "oldTuple": [],
    },
    {
        "transactionId": 2018,
        "commitTime": "1729503423666542",
        "table": "src_pl_dataset_202410210936594956.items",
        "op": "INSERT",
        "newTuple": [
            {
                "columnName": "col4",
                "columnType": 1184,
                "datumInt64": 1653312405176451,
            },
            {
                "columnName": "col9",
                "columnType": 3802,
                "datumString": (
                    '{"link": "?commen\\ntU\\nrn=urn%3Ali%3Acomment%3A%28acti\\n \\u0006 \\\\vity%3A69\'08444473\\n\\n551163392'
                    '%2C6n \\r \x8e9085", "complex": [1, 2, 3, "a"]}'
                ),
            },
            {
                "columnName": "col10",
                "columnType": 1082,
                "datumInt32": 19415,
            },
            {
                "columnName": "col11",
                "columnType": 1083,
                "datumInt64": 48405176451,
            },
            {"columnName": "col12", "columnType": 1114},
            {"columnName": "col13", "columnType": 700},
        ],
        "newTypeinfo": [
            {"modifier": "timestamp with time zone", "valueOptional": False},
            {"modifier": "jsonb", "valueOptional": False},
            {"modifier": "date", "valueOptional": False},
            {"modifier": "time without time zone", "valueOptional": False},
            {"modifier": "timestamp without time zone", "valueOptional": True},
            {"modifier": "real", "valueOptional": True},
        ],
    },
    {
        "transactionId": 932,
        "commitTime": "1729299383354856",
        "table": "src_pl_dataset_202410191256122080.tbl_x",
        "op": "DELETE",
        "oldTuple": [
            {
                "columnName": "id_x",
                "columnType": 20,
                "datumInt64": 1,
            },
            {
                "columnName": "val_x",
                "columnType": 1043,
            },
            {
                "columnName": "col_bool",
                "columnType": 16,
            },
            {
                "columnName": "col_bytea",
                "columnType": 17,
            },
            {
                "columnName": "col_int4",
                "columnType": 21,
            },
            {
                "columnName": "col_int",
                "columnType": 23,
            },
            {
                "columnName": "col_real",
                "columnType": 700,
            },
            {
                "columnName": "col_double",
                "columnType": 701,
            },
            {
                "columnName": "col_date",
                "columnType": 1082,
            },
            {
                "columnName": "col_time",
                "columnType": 1083,
            },
            {
                "columnName": "col_ts",
                "columnType": 1114,
            },
            {
                "columnName": "col_tstz",
                "columnType": 1184,
            },
            {
                "columnName": "col_num",
                "columnType": 1700,
            },
            {
                "columnName": "col_json",
                "columnType": 3802,
            },
        ],
    },
]

DATA_ITEMS: List[TDataItem] = [
    {
        "_dlt_id": "gGjifTMTAUs5ag",
        "_dlt_load_id": "1728662646.2657657",
        "id_y": 2,
        "val_y": False,
    },
    {
        "col4": pendulum.parse("2022-05-23T13:26:45.176451+00:00"),
        "col9": {
            "complex": [1, 2, 3, "a"],
            "link": (
                "?commen\ntU\nrn=urn%3Ali%3Acomment%3A%28acti\012 \6"
                " \\vity%3A69'08444473\n\n551163392%2C6n \r \x8e9085"
            ),
        },
        "col10": pendulum.parse("2023-02-27", strict=False).date(),
        "col11": pendulum.parse("13:26:45.176451", strict=False).time(),
        "col12": None,
        "col13": None,
    },
    {
        "id_x": 1,
        "val_x": "",
        "col_bool": True,
        "col_bytea": b" ",
        "col_int4": 0,
        "col_int": 0,
        "col_real": 0.0,
        "col_double": 0.0,
        "col_time": pendulum.parse("00:00:00", strict=False).time(),
        "col_date": pendulum.parse("1970-01-01", strict=False).date(),
        "col_ts": pendulum.parse("1970-01-01T00:00:00+00:00"),
        "col_tstz": pendulum.parse("1970-01-01T00:00:00+00:00"),
        "col_num": Decimal(0),
        "col_json": [0],
        "deleted_ts": pendulum.parse("2024-10-19T00:56:23.354856+00:00"),
    },
]

TABLE_SCHEMAS: List[TTableSchema] = [
    {
        "name": "tbl_y",
        "columns": {
            "id_y": {
                "data_type": "bigint",
                "name": "id_y",
                "nullable": False,
                "precision": 64,
            },
            "val_y": {"data_type": "bool", "name": "val_y", "nullable": True},
            "_dlt_id": {"data_type": "text", "name": "_dlt_id", "nullable": False},
            "_dlt_load_id": {
                "data_type": "text",
                "name": "_dlt_load_id",
                "nullable": False,
            },
            "lsn": {"data_type": "bigint", "name": "lsn", "nullable": True},
            "deleted_ts": {
                "data_type": "timestamp",
                "name": "deleted_ts",
                "nullable": True,
            },
        },
    },
    {
        "name": "items",
        "columns": {
            "col4": {"data_type": "timestamp", "name": "col4", "nullable": False},
            "col9": {"data_type": "complex", "name": "col9", "nullable": False},
            "col10": {"data_type": "date", "name": "col10", "nullable": False},
            "col11": {"data_type": "time", "name": "col11", "nullable": False},
            "col12": {"data_type": "timestamp", "name": "col12", "nullable": True},
            "col13": {"data_type": "double", "name": "col13", "nullable": True},
            "lsn": {"data_type": "bigint", "name": "lsn", "nullable": True},
            "deleted_ts": {
                "data_type": "timestamp",
                "name": "deleted_ts",
                "nullable": True,
            },
        },
    },
    {
        "name": "tbl_x",
        "columns": {
            "id_x": {
                "data_type": "bigint",
                "name": "id_x",
                "precision": 64,
                "nullable": False,
            },
            "val_x": {"data_type": "text", "name": "val_x"},
            "col_bool": {"data_type": "bool", "name": "col_bool"},
            "col_bytea": {"data_type": "binary", "name": "col_bytea"},
            "col_int4": {"data_type": "bigint", "name": "col_int4", "precision": 16},
            "col_int": {"data_type": "bigint", "name": "col_int", "precision": 32},
            "col_real": {"data_type": "double", "name": "col_real"},
            "col_double": {"data_type": "double", "name": "col_double"},
            "col_date": {"data_type": "date", "name": "col_date"},
            "col_time": {"data_type": "time", "name": "col_time"},
            "col_ts": {"data_type": "timestamp", "name": "col_ts"},
            "col_tstz": {"data_type": "timestamp", "name": "col_tstz"},
            "col_num": {"data_type": "decimal", "name": "col_num"},
            "col_json": {"data_type": "complex", "name": "col_json"},
            "lsn": {"data_type": "bigint", "name": "lsn", "nullable": True},
            "deleted_ts": {
                "data_type": "timestamp",
                "name": "deleted_ts",
                "nullable": True,
            },
        },
    },
]
