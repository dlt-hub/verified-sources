from sources.pg_legacy_replication.schema_types import _extract_table_schema
from sources.pg_legacy_replication.pg_logicaldec_pb2 import (
    RowMessage,
    Op,
    TypeInfo,
    DatumMessage,
)


def test_extract_table_schema():
    row_msg = RowMessage()
    row_msg.table = (
        'src_pl_dataset_202410010746423478."tbl_x_snapshot_00000003-00000149-1"'
    )
    row_msg.op = Op.INSERT
    row_msg.new_tuple.extend(
        [
            DatumMessage(column_name="id_x", column_type=20, datum_int64=1),
            DatumMessage(column_name="val_x", column_type=1043, datum_string="foo"),
            DatumMessage(
                column_name="_dlt_load_id",
                column_type=1043,
                datum_string="1727812002.3873408",
            ),
            DatumMessage(
                column_name="_dlt_id", column_type=1043, datum_string="EVvtapNpxpWbqA"
            ),
        ]
    )
    row_msg.new_typeinfo.extend(
        [
            TypeInfo(modifier="bigint"),
            TypeInfo(modifier="character varying", value_optional=True),
            TypeInfo(modifier="character varying", value_optional=True),
            TypeInfo(modifier="character varying", value_optional=True),
        ]
    )
    assert _extract_table_schema(row_msg) == {
        "name": "tbl_x",
        "columns": {
            "id_x": {
                "data_type": "bigint",
                "precision": 64,
                "name": "id_x",
                "nullable": False,
            },
            "val_x": {
                "data_type": "text",
                "name": "val_x",
                "nullable": True,
            },
            "_dlt_load_id": {
                "data_type": "text",
                "name": "_dlt_load_id",
                "nullable": True,
            },
            "_dlt_id": {
                "data_type": "text",
                "name": "_dlt_id",
                "nullable": True,
            },
        },
    }
