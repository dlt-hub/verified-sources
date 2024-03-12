from typing import List

from dlt.common import Decimal
from dlt.common.schema import TColumnSchema, TTableSchemaColumns


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
    {"name": "col9", "data_type": "complex", "nullable": False, "variant": True},
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
    {"name": "col9_null", "data_type": "complex", "nullable": True, "variant": True},
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
TABLE_UPDATE_COLUMNS_SCHEMA: TTableSchemaColumns = {t["name"]: t for t in TABLE_UPDATE}
