import json
import subprocess
import sys
from typing import Any, Dict, List, Optional

import bson
import dlt
import pyarrow as pa
import pytest
from bson import Decimal128, ObjectId
from pymongoarrow.types import Decimal128Type, ObjectIdType

from sources.mongodb.helpers import collection_documents
from sources.mongodb import worker_pool
from sources.mongodb.arrow import (
    ArrowBatchConverter,
    ArrowOptions,
    FieldTypeMismatchError,
)
from sources.mongodb.arrow_worker import (
    MongoArrowWorker,
    _supports_strict_schema,
    convert_arrow_columns,
    read_partition,
)
from sources.mongodb.worker_pool import worker_spec

from tests.mongodb.utils import (
    CODEC_OPTIONS,
    FakeCollection,
    decode_documents,
    extract,
    make_documents,
)


def test_arrow_loader_yields_each_batch_once() -> None:
    collection = FakeCollection(
        "docs", [{"_id": ObjectId(), "name": name} for name in "abc"]
    )

    tables = extract(collection, chunk_size=1, data_item_format="arrow")

    assert all(isinstance(table, pa.Table) for table in tables)
    assert sorted(table["name"].to_pylist() for table in tables) == [
        ["a"],
        ["b"],
        ["c"],
    ]


def test_arrow_matches_object_values() -> None:
    documents = make_documents(4)
    batch = b"".join(bson.encode(document) for document in documents)

    table = ArrowBatchConverter("docs", CODEC_OPTIONS, ArrowOptions()).convert([batch])
    objects = decode_documents(batch)

    for field in ("_id", "n", "title", "updated_at", "price", "payload"):
        assert table[field].to_pylist() == [row.get(field) for row in objects], field
    assert table["code"].to_pylist() == [str(row["code"]) for row in objects]
    assert table.schema.field("_id").type == pa.string()
    assert table.schema.field("updated_at").type == pa.timestamp("ms", tz="UTC")
    assert table.schema.field("payload").type == pa.binary()
    assert table.schema.field("code").type == pa.string()


def test_convert_arrow_columns_nested() -> None:
    oids = [ObjectId(), None, ObjectId()]
    oid_array = pa.ExtensionArray.from_storage(
        ObjectIdType(), pa.array([oid and oid.binary for oid in oids], pa.binary(12))
    )
    decimals = [Decimal128("1.5"), None, Decimal128("-0.01")]
    decimal_array = pa.ExtensionArray.from_storage(
        Decimal128Type(), pa.array([dec and dec.bid for dec in decimals], pa.binary(16))
    )
    table = pa.table(
        {
            "_id": oid_array,
            "nested": pa.StructArray.from_arrays(
                [oid_array, decimal_array],
                names=["oid", "dec"],
                mask=pa.array([False, False, True]),
            ),
            "ids": pa.ListArray.from_arrays(
                pa.array([0, 2, 2, 3], pa.int32()),
                oid_array,
                mask=pa.array([False, True, False]),
            ),
        }
    )
    ids = [str(oid) if oid else None for oid in oids]
    expected = {
        "_id": ids,
        "nested": [{"oid": ids[0], "dec": "1.5"}, {"oid": None, "dec": None}, None],
        "ids": [[ids[0], None], None, [ids[2]]],
    }

    converted = convert_arrow_columns(table)
    assert converted.schema == pa.schema(
        {
            "_id": pa.string(),
            "nested": pa.struct({"oid": pa.string(), "dec": pa.string()}),
            "ids": pa.list_(pa.string()),
        }
    )
    assert converted.to_pydict() == expected

    # sliced and multi chunk columns
    chunked = pa.concat_tables([table.slice(1), table.slice(0, 2)])
    assert convert_arrow_columns(chunked).to_pydict() == {
        column: values[1:] + values[:2] for column, values in expected.items()
    }
    assert convert_arrow_columns(table.slice(0, 0)).num_rows == 0


def test_read_partition() -> None:
    documents = make_documents(250)
    for document in documents:
        document["v"] = document["n"]
    # in the third raw batch of the fake server
    documents[240]["v"] = "text"
    collection = FakeCollection("docs", documents)

    table, mismatch = read_partition(
        collection,
        {"n": {"$gte": 0}},
        None,
        pymongoarrow_schema=None,
        detect_mismatch=False,
    )

    (cursor,) = collection.cursors
    assert cursor.filter == {"n": {"$gte": 0}}
    assert mismatch is None
    # raw batches are parsed together, the type of `v` is inferred once for the partition
    assert table.num_rows == 250
    assert pa.types.is_integer(table.schema.field("v").type)
    assert table["v"][240].as_py() is None
    # BSON extension types are converted in the worker, plain Arrow types cross the processes
    assert table.schema.field("_id").type == pa.string()
    assert table.schema.field("price").type == pa.string()


def test_mongo_worker_imports_no_dlt() -> None:
    # loads the worker like `serve` in a worker process does
    probe = (
        "import json, runpy, sys; sys.path[:] = json.loads(sys.argv[1]); "
        "runpy.run_path(sys.argv[2])['load_worker'](*json.loads(sys.argv[3])); "
        "print(sorted(m for m in ('dlt', 'sources') if m in sys.modules))"
    )

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            probe,
            json.dumps(sys.path),
            worker_pool.__file__,
            json.dumps(worker_spec(MongoArrowWorker)),
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == "[]"


requires_strict_schema = pytest.mark.skipif(
    not _supports_strict_schema(), reason="pymongoarrow<1.9 has no strict schema"
)


@requires_strict_schema
@pytest.mark.parametrize(
    "values,fields",
    [
        ([1, "a", 2.5], {"v": ["float", "int", "str"]}),
        ([{"x": 1}, {"x": "a"}], {"v.x": ["int", "str"]}),
        ([[1, 2], [1, "a"]], {"v[]": ["int", "str"]}),
    ],
    ids=["scalar", "nested_document", "array"],
)
def test_type_mismatch_in_batch_raises(
    values: List[Any], fields: Dict[str, List[str]]
) -> None:
    collection = FakeCollection(
        "docs", [{"_id": n, "v": value} for n, value in enumerate(values)]
    )

    with pytest.raises(Exception) as ex:
        extract(
            collection,
            data_item_format="arrow",
            on_inferred_type_mismatch="raise",
        )
    error = ex.value
    while not isinstance(error, FieldTypeMismatchError):
        error = error.__cause__ or error.__context__
    assert error.collection_name == "docs"
    assert error.fields == fields
    assert "pymongoarrow_schema" in str(error)


@pytest.mark.parametrize(
    "values,on_inferred_type_mismatch,expected,warned",
    [
        ([1, 2.5], "ignore", [[1], [2]], False),
        ([2.5, 3], "ignore", [[2.5], [3.0]], False),
        ([1, "a"], "ignore", [[1], [None]], False),
        ([1, 2**40], "raise", [[1], [2**40]], False),
        ([1, 2.5], "warn", [[1], [2]], True),
        ([1, 2.5], "raise", "`v` (int32, double)", False),
        ([1, "a"], "raise", "`v` (int32, string)", False),
    ],
    ids=[
        "int_to_double",
        "double_to_int",
        "int_to_string",
        "wider_int",
        "warn",
        "raise_number",
        "raise_string",
    ],
)
def test_type_mismatch_across_batches(
    values: List[Any],
    on_inferred_type_mismatch: str,
    expected: Any,
    warned: bool,
    mocker: Any,
) -> None:
    warning = mocker.patch("dlt.common.logger.warning")
    collection = FakeCollection(
        "docs", [{"_id": n, "v": value} for n, value in enumerate(values)]
    )

    def tables() -> List[Any]:
        return extract(
            collection,
            data_item_format="arrow",
            chunk_size=1,
            on_inferred_type_mismatch=on_inferred_type_mismatch,
        )

    if isinstance(expected, str):
        with pytest.raises(Exception) as ex:
            tables()
        assert expected in str(ex.value)
        return
    # numbers are cast to the first type of the field, other values load as null
    assert [table["v"].to_pylist() for table in tables()] == expected
    warnings = [call for call in warning.call_args_list if "`v`" in str(call)]
    assert len(warnings) == warned


@pytest.mark.parametrize(
    "on_inferred_type_mismatch",
    ["warn", "ignore", None],
    ids=["warn", "ignore", "default"],
)
def test_type_mismatch_loads_null(
    on_inferred_type_mismatch: Optional[str], mocker: Any
) -> None:
    warning = mocker.patch("dlt.common.logger.warning")
    # one batch with mixed values, then a batch with the other type
    collection = FakeCollection(
        "docs",
        [
            {"_id": 1, "v": 1},
            {"_id": 2, "v": "a"},
            {"_id": 3, "v": 2},
            {"_id": 4, "v": "b"},
        ],
    )

    kwargs = (
        {"on_inferred_type_mismatch": on_inferred_type_mismatch}
        if on_inferred_type_mismatch
        else {}
    )
    tables = extract(collection, data_item_format="arrow", chunk_size=3, **kwargs)

    assert [table["v"].to_pylist() for table in tables] == [[1, None, 2], [None]]
    warned = [call for call in warning.call_args_list if "different types" in str(call)]
    if on_inferred_type_mismatch == "warn" and _supports_strict_schema():
        # warned once, also when the field mismatches again in the next batch
        assert len(warned) == 1
        assert "`v`" in str(warned[0])
    if on_inferred_type_mismatch != "warn":
        assert warned == []


@requires_strict_schema
def test_type_mismatch_with_pymongoarrow_schema() -> None:
    from pymongoarrow.api import Schema

    schema = Schema({"_id": pa.int64(), "v": pa.int64()})
    documents = [{"_id": 1, "v": 1}, {"_id": 2, "v": "a"}]

    # the schema is enforced whatever `on_inferred_type_mismatch` says
    for on_inferred_type_mismatch in ("raise", "ignore"):
        with pytest.raises(Exception) as ex:
            extract(
                FakeCollection("docs", documents),
                data_item_format="arrow",
                pymongoarrow_schema=schema,
                on_inferred_type_mismatch=on_inferred_type_mismatch,
            )
        assert "do not match `pymongoarrow_schema`: `v` (int, str)" in str(ex.value)

    tables = extract(
        FakeCollection("docs", [{"_id": 1, "v": 1}]),
        data_item_format="arrow",
        pymongoarrow_schema=schema,
    )
    assert tables[0]["v"].to_pylist() == [1]


@pytest.mark.parametrize(
    "max_table_nesting,columns",
    [
        (None, ["_id", "camel_case__x", "camel_case__inner__y", "tags"]),
        (1, ["_id", "camel_case__x", "camel_case__inner", "tags"]),
        (0, ["_id", "camel_case", "tags"]),
    ],
    ids=["default_nesting", "nesting_1", "nesting_0"],
)
def test_arrow_flattens_documents_like_object_format(
    max_table_nesting: Optional[int], columns: List[str]
) -> None:
    collection = FakeCollection(
        "docs", [{"_id": 1, "camelCase": {"x": 1, "inner": {"y": "a"}}, "tags": ["t"]}]
    )

    table = extract(
        collection, data_item_format="arrow", max_table_nesting=max_table_nesting
    )[0]

    assert table.column_names == columns
    assert table["tags"].to_pylist() == [["t"]]


def test_arrow_keeps_json_columns() -> None:
    collection = FakeCollection("docs", [{"_id": 1, "doc": {"x": 1}}])

    table = extract(
        collection, data_item_format="arrow", columns={"doc": {"data_type": "json"}}
    )[0]

    assert table.column_names == ["_id", "doc"]
    assert table["doc"].to_pylist() == [{"x": 1}]


def test_arrow_flattened_name_collision_raises() -> None:
    collection = FakeCollection("docs", [{"_id": 1, "a": {"b": 1}, "a__b": 2}])

    with pytest.raises(Exception) as ex:
        extract(collection, data_item_format="arrow")
    assert "duplicate columns: ['a__b']" in str(ex.value)

    # nested documents that are not flattened do not collide
    table = extract(
        FakeCollection("docs", [{"_id": 1, "a": {"b": 1}, "a__b": 2}]),
        data_item_format="arrow",
        max_table_nesting=0,
    )[0]
    assert table.column_names == ["_id", "a", "a__b"]


def test_arrow_with_dynamic_hints() -> None:
    collection = FakeCollection(
        "docs", [{"_id": 1, "doc": {"x": 1}}, {"_id": 2, "doc": {"x": 2}}]
    )
    resource = dlt.resource(
        collection_documents,
        name="docs",
        table_name=lambda table: f"docs_{table.num_rows}",
        columns=lambda table: {"_id": {"data_type": "bigint"}},
        max_table_nesting=0,
    )

    tables = list(
        resource(
            None,
            collection,
            filter_={},
            projection=None,
            pymongoarrow_schema=None,
            data_item_format="arrow",
        ).add_limit(1)
    )

    # the nesting comes from the resource without evaluating its dynamic hints
    assert tables[0].column_names == ["_id", "doc"]


@pytest.mark.parametrize("null_id", [False, True], ids=["ids", "null_id"])
def test_arrow_id_not_nullable(null_id: bool, mocker: Any) -> None:
    warning = mocker.patch("dlt.common.logger.warning")
    documents = make_documents(3)
    if null_id:
        # valid in MongoDB, dlt rejects it as the primary key
        documents[0]["_id"] = None
    resource = dlt.resource(collection_documents, name="docs", primary_key="_id")(
        None,
        FakeCollection("docs", documents),
        filter_={},
        projection=None,
        pymongoarrow_schema=None,
        data_item_format="arrow",
    )

    (table,) = list(resource)
    assert table.schema.field("_id").nullable == null_id
    if not null_id:
        dlt.pipeline(pipeline_name="arrow_id", dev_mode=True).extract(resource)
        assert not [
            c for c in warning.call_args_list if "hints were different" in str(c)
        ]
