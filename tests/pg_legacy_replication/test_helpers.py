from typing import Optional

import pendulum
import pytest
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TDataItem
from google.protobuf.json_format import ParseDict as parse_dict

from sources.pg_legacy_replication.helpers import (
    infer_table_schema,
    gen_data_item,
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
    assert gen_data_item(row_msg, column_schema) == data_item
