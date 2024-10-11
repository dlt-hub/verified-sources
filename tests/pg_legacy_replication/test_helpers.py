from sources.pg_legacy_replication.helpers import extract_table_schema, gen_data_item
from sources.pg_legacy_replication.pg_logicaldec_pb2 import RowMessage
from google.protobuf.json_format import ParseDict as parse_dict


def test_extract_table_schema():
    row_msg = RowMessage()
    data = {
        "transactionId": 969,
        "commitTime": "1728662646949062",
        "table": "src_pl_dataset_202410110404048747_staging.tbl_y",
        "op": "INSERT",
        "newTuple": [
            {
                "columnName": "id_y",
                "columnType": "20",
                "datumInt64": 2,
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
    }
    parse_dict(data, row_msg)
    assert extract_table_schema(row_msg) == {
        "name": "tbl_y",
        "columns": {
            "id_y": {
                "data_type": "bigint",
                "precision": 64,
                "name": "id_y",
                "nullable": False,
            },
            "val_y": {
                "data_type": "bool",
                "name": "val_y",
                "nullable": True,
            },
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
        },
    }


def test_gen_data_item():
    row_msg = RowMessage()
    data = {
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
    }
    parse_dict(data, row_msg)
    table_schema = extract_table_schema(row_msg)
    assert gen_data_item(row_msg, table_schema["columns"], lsn=27078296) == {
        "_dlt_id": "gGjifTMTAUs5ag",
        "_dlt_load_id": "1728662646.2657657",
        "id_y": 2,
        "lsn": 27078296,
        "val_y": False,
    }
