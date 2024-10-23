import random
from typing import Optional

import pendulum
import pytest
from dlt.common.schema.typing import TTableSchema, TTableSchemaColumns
from dlt.common.typing import TDataItem
from google.protobuf.json_format import ParseDict as parse_dict

from sources.pg_legacy_replication.helpers import (
    infer_table_schema,
    gen_data_item,
)
from sources.pg_legacy_replication.pg_logicaldec_pb2 import RowMessage


@pytest.mark.parametrize(
    "data, table_hints, expected_schema",
    [
        (
            {
                "transactionId": 969,
                "commitTime": "1728662646949062",
                "table": "src_pl_dataset_202410110404048747_staging.tbl_y",
                "op": "INSERT",
                "newTuple": [
                    {"columnName": "id_y", "columnType": "20", "datumInt64": 2},
                    {"columnName": "val_y", "columnType": "16", "datumBool": False},
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
                    {"modifier": "bigint", "valueOptional": False},
                    {"modifier": "boolean", "valueOptional": True},
                    {"modifier": "character varying", "valueOptional": False},
                    {"modifier": "character varying", "valueOptional": False},
                ],
                "oldTuple": [],
            },
            {"columns": {"id_y": {"primary_key": True}}, "write_disposition": "merge"},
            {
                "name": "tbl_y",
                "columns": {
                    "id_y": {
                        "data_type": "bigint",
                        "precision": 64,
                        "name": "id_y",
                        "nullable": False,
                        "primary_key": True,
                    },
                    "val_y": {"data_type": "bool", "name": "val_y", "nullable": True},
                    "_dlt_load_id": {
                        "data_type": "text",
                        "name": "_dlt_load_id",
                        "nullable": False,
                    },
                    "_dlt_id": {
                        "data_type": "text",
                        "name": "_dlt_id",
                        "nullable": False,
                    },
                    "lsn": {
                        "data_type": "bigint",
                        "dedup_sort": "desc",
                        "nullable": True,
                    },
                    "deleted_ts": {
                        "data_type": "timestamp",
                        "hard_delete": True,
                        "nullable": True,
                    },
                },
                "write_disposition": "merge",
            },
        ),
    ],
)
def test_infer_table_schema(
    data,
    table_hints: Optional[TTableSchema],
    expected_schema: TTableSchema,
):
    row_msg = RowMessage()
    parse_dict(data, row_msg)
    assert infer_table_schema(row_msg, table_hints=table_hints) == expected_schema


LSN = random.randint(0, 10000)


@pytest.mark.parametrize(
    "data, data_item",
    [
        (
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
                "_dlt_id": "gGjifTMTAUs5ag",
                "_dlt_load_id": "1728662646.2657657",
                "id_y": 2,
                "val_y": False,
            },
        ),
        (
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
                ],
                "newTypeinfo": [
                    {"modifier": "timestamp with time zone", "valueOptional": False},
                    {"modifier": "jsonb", "valueOptional": False},
                    {"modifier": "date", "valueOptional": False},
                    {"modifier": "time without time zone", "valueOptional": False},
                ],
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
            },
        ),
    ],
)
def test_gen_data_item(data, data_item: TDataItem):
    row_msg = RowMessage()
    parse_dict(data, row_msg)
    column_schema = infer_table_schema(row_msg)["columns"]
    assert gen_data_item(row_msg.new_tuple, column_schema=column_schema) == data_item


@pytest.mark.parametrize(
    "data, data_item",
    [
        (
            {
                "transactionId": 932,
                "commitTime": "1729299383354856",
                "table": "src_pl_dataset_202410191256122080.tbl_x",
                "op": "DELETE",
                "oldTuple": [
                    {
                        "columnName": "id_x",
                        "columnType": "20",
                        "datumInt64": "1",
                    },
                    {
                        "columnName": "val_x",
                        "columnType": "1043",
                    },
                    {
                        "columnName": "_dlt_load_id",
                        "columnType": "1043",
                    },
                    {
                        "columnName": "_dlt_id",
                        "columnType": "1043",
                    },
                ],
            },
            {"id_x": 1, "val_x": "", "_dlt_load_id": "", "_dlt_id": ""},
        ),
    ],
)
def test_gen_delete_item(data, data_item: TDataItem):
    row_msg = RowMessage()
    parse_dict(data, row_msg)
    assert gen_data_item(row_msg.old_tuple, for_delete=True) == data_item
