import pytest
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TDataItem
from google.protobuf.json_format import ParseDict as parse_dict
from sources.pg_legacy_replication.helpers import (
    compare_schemas,
    gen_data_item,
    infer_table_schema,
)
from sources.pg_legacy_replication.pg_logicaldec_pb2 import RowMessage
from .cases import (
    DATA_ITEMS,
    ROW_MESSAGES,
    SIMILAR_SCHEMAS,
    TABLE_SCHEMAS,
    SchemaChoice,
)


@pytest.mark.parametrize("data, expected_schema", zip(ROW_MESSAGES, TABLE_SCHEMAS))
def test_infer_table_schema(
    data,
    expected_schema: TTableSchema,
):
    row_msg = RowMessage()
    parse_dict(data, row_msg)
    assert (
        infer_table_schema(
            row_msg,
            include_commit_ts=True,
            include_tx_id=True,
        )
        == expected_schema
    )


@pytest.mark.parametrize("data, data_item", zip(ROW_MESSAGES, DATA_ITEMS))
def test_gen_data_item(data, data_item: TDataItem):
    row_msg = RowMessage()
    parse_dict(data, row_msg)
    column_schema = infer_table_schema(row_msg)["columns"]
    assert (
        gen_data_item(
            row_msg,
            column_schema,
            lsn=1,
            include_commit_ts=True,
            include_tx_id=True,
        )
        == data_item
    )


@pytest.mark.parametrize("s1, s2, choice", SIMILAR_SCHEMAS)
def test_compare_schemas(s1: TTableSchema, s2: TTableSchema, choice: SchemaChoice):
    if choice == SchemaChoice.error:
        with pytest.raises(AssertionError):
            compare_schemas(s1, s2)
        with pytest.raises(AssertionError):
            compare_schemas(s2, s1)
    else:
        expected_schema = (s1, s2)[choice]
        assert compare_schemas(s1, s2) == expected_schema
        assert compare_schemas(s2, s1) == expected_schema
