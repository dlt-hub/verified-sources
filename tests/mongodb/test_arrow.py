from typing import Any, Dict, List, Optional

import bson
import dlt
import pyarrow as pa
import pytest
from bson import Decimal128, ObjectId
from pymongoarrow.types import Decimal128Type, ObjectIdType

from sources.mongodb.helpers import collection_documents
from sources.mongodb.arrow import (
    ArrowBatchConverter,
    ArrowOptions,
    CollectionArrowLoaderParallel,
    FieldTypeMismatchError,
    _supports_strict_schema,
    convert_arrow_columns,
)

from tests.mongodb.utils import (
    CODEC_OPTIONS,
    FakeCollection,
    decode_documents,
    extract,
    make_documents,
)


@pytest.mark.parametrize("parallel", [False, True], ids=["sequential", "parallel"])
def test_arrow_loader_yields_each_batch_once(parallel: bool) -> None:
    collection = FakeCollection(
        "docs", [{"_id": ObjectId(), "name": name} for name in "abc"]
    )

    tables = extract(
        collection, parallel=parallel, chunk_size=1, data_item_format="arrow"
    )

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


def test_arrow_run_batch_returns_table() -> None:
    collection = FakeCollection("docs", make_documents(3))
    loader = CollectionArrowLoaderParallel(None, collection, chunk_size=2)
    cursor = loader._get_cursor(filter_={})
    converter = ArrowBatchConverter("docs", CODEC_OPTIONS, ArrowOptions())

    # deferred batches run in the thread pool only when they return data, not a generator
    table = loader._run_batch(
        cursor=cursor, batch={"skip": 0, "limit": 2}, converter=converter
    )()
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert (
        loader._run_batch(
            cursor=cursor, batch={"skip": 3, "limit": 2}, converter=converter
        )()
        is None
    )


requires_strict_schema = pytest.mark.skipif(
    not _supports_strict_schema(), reason="pymongoarrow<1.9 has no strict schema"
)


@requires_strict_schema
@pytest.mark.parametrize("parallel", [False, True], ids=["sequential", "parallel"])
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
    values: List[Any], fields: Dict[str, List[str]], parallel: bool
) -> None:
    collection = FakeCollection(
        "docs", [{"_id": n, "v": value} for n, value in enumerate(values)]
    )

    with pytest.raises(Exception) as ex:
        extract(
            collection,
            data_item_format="arrow",
            parallel=parallel,
            on_inferred_type_mismatch="raise",
        )
    error = ex.value
    while not isinstance(error, FieldTypeMismatchError):
        error = error.__cause__ or error.__context__
    assert error.collection_name == "docs"
    assert error.fields == fields
    assert "pymongoarrow_schema" in str(error)


def test_type_mismatch_across_batches_raises() -> None:
    collection = FakeCollection("docs", [{"_id": 1, "v": 1}, {"_id": 2, "v": "a"}])

    with pytest.raises(Exception) as ex:
        extract(
            collection,
            data_item_format="arrow",
            chunk_size=1,
            on_inferred_type_mismatch="raise",
        )
    assert "`v` (int32, string)" in str(ex.value)


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


def test_type_mismatch_allows_wider_integers() -> None:
    collection = FakeCollection("docs", [{"_id": 1, "v": 1}, {"_id": 2, "v": 2**40}])

    tables = extract(collection, data_item_format="arrow", chunk_size=1)

    assert [table["v"].to_pylist() for table in tables] == [[1], [2**40]]


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
