import pytest
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TDataItem
from google.protobuf.json_format import ParseDict as parse_dict

from sources.pg_legacy_replication.helpers import (
    infer_table_schema,
    gen_data_item,
    compare_schemas,
)
from sources.pg_legacy_replication.pg_logicaldec_pb2 import RowMessage
from .cases import ROW_MESSAGES, DATA_ITEMS, TABLE_SCHEMAS


@pytest.mark.parametrize("data, expected_schema", zip(ROW_MESSAGES, TABLE_SCHEMAS))
def test_infer_table_schema(
    data,
    expected_schema: TTableSchema,
):
    row_msg = RowMessage()
    parse_dict(data, row_msg)
    assert infer_table_schema(row_msg) == expected_schema


@pytest.mark.parametrize("data, data_item", zip(ROW_MESSAGES, DATA_ITEMS))
def test_gen_data_item(data, data_item: TDataItem):
    row_msg = RowMessage()
    parse_dict(data, row_msg)
    column_schema = infer_table_schema(row_msg)["columns"]
    assert gen_data_item(row_msg, column_schema, lsn=1) == data_item


def test_compare_schemas():
    s1: TTableSchema = {
        "name": "items",
        "columns": {
            "col1": {
                "name": "col1",
                "data_type": "bigint",
                "precision": 64,
                "nullable": False,
            },
            "col2": {"name": "col2", "data_type": "double", "nullable": False},
            "col3": {"name": "col3", "data_type": "bool", "nullable": False},
            "col4": {"name": "col4", "data_type": "timestamp", "nullable": False},
            "col5": {"name": "col5", "data_type": "text", "nullable": False},
            "col6": {
                "name": "col6",
                "data_type": "decimal",
                "precision": 38,
                "scale": 9,
                "nullable": False,
            },
            "col7": {"name": "col7", "data_type": "binary", "nullable": False},
            "col9": {"name": "col9", "data_type": "complex", "nullable": False},
            "col10": {"name": "col10", "data_type": "date", "nullable": False},
            "col11": {"name": "col11", "data_type": "time", "nullable": False},
            "col1_null": {
                "name": "col1_null",
                "data_type": "bigint",
                "precision": 64,
                "nullable": True,
            },
            "col2_null": {"name": "col2_null", "data_type": "double", "nullable": True},
            "col3_null": {"name": "col3_null", "data_type": "bool", "nullable": True},
            "col4_null": {
                "name": "col4_null",
                "data_type": "timestamp",
                "nullable": True,
            },
            "col5_null": {"name": "col5_null", "data_type": "text", "nullable": True},
            "col6_null": {
                "name": "col6_null",
                "data_type": "decimal",
                "precision": 38,
                "scale": 9,
                "nullable": True,
            },
            "col7_null": {"name": "col7_null", "data_type": "binary", "nullable": True},
            "col9_null": {
                "name": "col9_null",
                "data_type": "complex",
                "nullable": True,
            },
            "col10_null": {"name": "col10_null", "data_type": "date", "nullable": True},
            "col11_null": {"name": "col11_null", "data_type": "time", "nullable": True},
            "col1_precision": {
                "name": "col1_precision",
                "data_type": "bigint",
                "precision": 16,
                "nullable": False,
            },
            "col4_precision": {
                "name": "col4_precision",
                "data_type": "timestamp",
                "precision": 3,
                "nullable": False,
            },
            "col5_precision": {
                "name": "col5_precision",
                "data_type": "text",
                "precision": 25,
                "nullable": False,
            },
            "col6_precision": {
                "name": "col6_precision",
                "data_type": "decimal",
                "precision": 6,
                "scale": 2,
                "nullable": False,
            },
            "col7_precision": {
                "name": "col7_precision",
                "data_type": "binary",
                "nullable": False,
            },
            "col11_precision": {
                "name": "col11_precision",
                "data_type": "time",
                "precision": 3,
                "nullable": False,
            },
            "_dlt_load_id": {
                "name": "_dlt_load_id",
                "data_type": "text",
                "nullable": False,
            },
            "_dlt_id": {"name": "_dlt_id", "data_type": "text", "nullable": False},
            "_pg_lsn": {"data_type": "bigint", "name": "_pg_lsn", "nullable": True},
            "_pg_deleted_ts": {
                "data_type": "timestamp",
                "name": "_pg_deleted_ts",
                "nullable": True,
            },
        },
    }
    s2: TTableSchema = {
        "name": "items",
        "columns": {
            "col1": {
                "name": "col1",
                "data_type": "bigint",
                "precision": 64,
                "nullable": False,
            },
            "col2": {"name": "col2", "data_type": "double"},
            "col3": {"name": "col3", "data_type": "bool"},
            "col4": {"name": "col4", "data_type": "timestamp"},
            "col5": {"name": "col5", "data_type": "text"},
            "col6": {"name": "col6", "data_type": "decimal"},
            "col7": {"name": "col7", "data_type": "binary"},
            "col9": {"name": "col9", "data_type": "complex"},
            "col10": {"name": "col10", "data_type": "date"},
            "col11": {"name": "col11", "data_type": "time"},
            "col1_null": {"name": "col1_null", "data_type": "bigint", "precision": 64},
            "col2_null": {"name": "col2_null", "data_type": "double"},
            "col3_null": {"name": "col3_null", "data_type": "bool"},
            "col4_null": {"name": "col4_null", "data_type": "timestamp"},
            "col5_null": {"name": "col5_null", "data_type": "text"},
            "col6_null": {"name": "col6_null", "data_type": "decimal"},
            "col7_null": {"name": "col7_null", "data_type": "binary"},
            "col9_null": {"name": "col9_null", "data_type": "complex"},
            "col10_null": {"name": "col10_null", "data_type": "date"},
            "col11_null": {"name": "col11_null", "data_type": "time"},
            "col1_precision": {
                "name": "col1_precision",
                "data_type": "bigint",
                "precision": 16,
            },
            "col4_precision": {"name": "col4_precision", "data_type": "timestamp"},
            "col5_precision": {"name": "col5_precision", "data_type": "text"},
            "col6_precision": {"name": "col6_precision", "data_type": "decimal"},
            "col7_precision": {"name": "col7_precision", "data_type": "binary"},
            "col11_precision": {"name": "col11_precision", "data_type": "time"},
            "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text"},
            "_dlt_id": {"name": "_dlt_id", "data_type": "text"},
            "_pg_lsn": {"data_type": "bigint", "name": "_pg_lsn", "nullable": True},
            "_pg_deleted_ts": {
                "data_type": "timestamp",
                "name": "_pg_deleted_ts",
                "nullable": True,
            },
        },
    }
    assert compare_schemas(s1, s2) == s1
    assert compare_schemas(s2, s1) == s1

    s1 = {
        "columns": {
            "_dlt_id": {"data_type": "text", "name": "_dlt_id", "nullable": False},
            "_dlt_load_id": {
                "data_type": "text",
                "name": "_dlt_load_id",
                "nullable": False,
            },
            "c1": {
                "data_type": "bigint",
                "name": "c1",
                "nullable": True,
                "precision": 64,
            },
            "c2": {
                "data_type": "bigint",
                "name": "c2",
                "nullable": True,
                "precision": 64,
            },
            "c3": {
                "data_type": "bigint",
                "name": "c3",
                "nullable": True,
                "precision": 64,
            },
            "_pg_deleted_ts": {
                "data_type": "timestamp",
                "name": "_pg_deleted_ts",
                "nullable": True,
            },
            "_pg_lsn": {"data_type": "bigint", "name": "_pg_lsn", "nullable": True},
        },
        "name": "items",
    }
    from copy import deepcopy

    s2 = deepcopy(s1)
    s2["columns"]["c4"] = {
        "data_type": "bigint",
        "name": "c4",
        "nullable": True,
        "precision": 64,
    }
    with pytest.raises(AssertionError):
        compare_schemas(s1, s2)
