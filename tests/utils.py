import os
import sys
import platform
import pytest
from typing import Any, Iterator, List, Sequence, Dict, Optional, Set
from os import environ
from unittest.mock import patch

import dlt
from dlt.common import json, known_env
from dlt.common.data_types import py_type_to_sc_type
from dlt.common.typing import DictStrAny, TDataItem
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import PluggableRunContext
from dlt.common.configuration.providers import (
    EnvironProvider,
    ConfigTomlProvider,
    SecretsTomlProvider,
)
from dlt.common.configuration.specs.pluggable_run_context import (
    SupportsRunContext,
)
from dlt.common.runtime.run_context import DOT_DLT, RunContext
from dlt.common.pipeline import LoadInfo, PipelineContext, ExtractInfo
from dlt.common.storages import FileStorage
from dlt.common.schema.typing import TTableSchema
from dlt.common.utils import set_working_dir

from dlt.common.destination.exceptions import SqlClientNotAvailable

TEST_STORAGE_ROOT = "_storage"

# get env variable with destinations
ALL_DESTINATIONS = dlt.config.get("ALL_DESTINATIONS", list) or [
    "duckdb",
]
# ALL_DESTINATIONS = ["duckdb"]
# ALL_DESTINATIONS = ["bigquery"]

skipifwindows = pytest.mark.skipif(
    platform.system() == "Windows", reason="does not runs on windows"
)


@pytest.fixture(autouse=True)
def drop_pipeline() -> Iterator[None]:
    """Drops all the datasets for the pipeline created in the test. Does not drop the working dir - see new_test_storage"""
    yield
    drop_active_pipeline_data()


@pytest.fixture(autouse=True, scope="session")
def test_config_providers():
    """Creates set of config providers where tomls are loaded from tests/.dlt"""
    config_root = "./sources/.dlt"

    # inject provider context so the original providers are restored at the end
    def _initial_providers(self):
        return [
            EnvironProvider(),
            SecretsTomlProvider(settings_dir=config_root),
            ConfigTomlProvider(settings_dir=config_root),
        ]

    with patch(
        "dlt.common.runtime.run_context.RunContext.initial_providers",
        _initial_providers,
    ):
        Container()[PluggableRunContext].reload_providers()
        yield


class MockableRunContext(RunContext):
    @property
    def name(self) -> str:
        return self._name

    @property
    def global_dir(self) -> str:
        return self._global_dir

    @property
    def run_dir(self) -> str:
        return os.environ.get(known_env.DLT_PROJECT_DIR, self._run_dir)

    @property
    def data_dir(self) -> str:
        return os.environ.get(known_env.DLT_DATA_DIR, self._data_dir)

    _name: str
    _global_dir: str
    _run_dir: str
    _settings_dir: str
    _data_dir: str

    @classmethod
    def from_context(cls, ctx: SupportsRunContext) -> "MockableRunContext":
        cls_ = cls(ctx.run_dir)
        cls_._name = ctx.name
        cls_._global_dir = ctx.global_dir
        cls_._run_dir = ctx.run_dir
        cls_._settings_dir = ctx.settings_dir
        cls_._data_dir = ctx.data_dir
        return cls_


@pytest.fixture(autouse=True)
def patch_pipeline_working_dir() -> Iterator[None]:
    """Puts the pipeline working directory into test storage"""
    ctx = PluggableRunContext()
    mock = MockableRunContext.from_context(ctx.context)
    mock._global_dir = mock._data_dir = os.path.join(
        os.path.abspath(TEST_STORAGE_ROOT), DOT_DLT
    )
    ctx.context = mock

    with Container().injectable_context(ctx):
        yield


@pytest.fixture(autouse=True)
def new_test_storage() -> FileStorage:
    """Creates new empty test storage in ./_storage folder"""
    return clean_test_storage()


@pytest.fixture(scope="function", autouse=True)
def preserve_environ() -> Iterator[None]:
    """Restores the environ after the test was run"""
    saved_environ = environ.copy()
    yield
    environ.clear()
    environ.update(saved_environ)


def drop_active_pipeline_data() -> None:
    """Drops all the datasets for currently active pipeline and then deactivated it. Does not drop the working dir - see new_test_storage"""
    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()

        def _drop_dataset(schema_name: str) -> None:
            try:
                with p.sql_client(schema_name) as c:
                    try:
                        c.drop_dataset()
                        # print("dropped")
                    except Exception as exc:
                        print(exc)
                    with c.with_staging_dataset():
                        try:
                            c.drop_dataset()
                            # print("dropped")
                        except Exception as exc:
                            print(exc)
            except SqlClientNotAvailable:
                pass

        # take all schemas and if destination was set
        if p.destination:
            if p.config.use_single_dataset:
                # drop just the dataset for default schema
                if p.default_schema_name:
                    _drop_dataset(p.default_schema_name)
            else:
                # for each schema, drop the dataset
                for schema_name in p.schema_names:
                    _drop_dataset(schema_name)
        # p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()


def clean_test_storage(
    init_normalize: bool = False, init_loader: bool = False, mode: str = "t"
) -> FileStorage:
    storage = FileStorage(TEST_STORAGE_ROOT, mode, makedirs=True)
    storage.delete_folder("", recursively=True, delete_ro=True)
    storage.create_folder(".")
    if init_normalize:
        from dlt.common.storages import NormalizeStorage

        NormalizeStorage(True)
    if init_loader:
        from dlt.common.storages import LoadStorage

        LoadStorage(True, LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    return storage


def assert_table_data(
    p: dlt.Pipeline,
    table_name: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
) -> None:
    """Asserts that table contains single column (ordered ASC) of values matches `table_data`. If `info` is provided, second column must contain one of load_ids in `info`"""
    assert_query_data(
        p,
        f"SELECT * FROM {table_name} ORDER BY 1 NULLS FIRST",
        table_data,
        schema_name,
        info,
    )


def assert_query_data(
    p: dlt.Pipeline,
    sql: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
) -> None:
    """Asserts that query selecting single column of values matches `table_data`. If `info` is provided, second column must contain one of load_ids in `info`"""
    with p.sql_client(schema_name=schema_name) as c:
        with c.execute_query(sql) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == len(table_data)
            for row, d in zip(rows, table_data):
                row = list(row)
                # first element comes from the data
                assert row[0] == d
                # the second is load id
                if info:
                    assert row[1] in info.loads_ids


def assert_load_info(info: LoadInfo, expected_load_packages: int = 1) -> None:
    """Asserts that expected number of packages was loaded and there are no failed jobs"""
    assert len(info.loads_ids) == expected_load_packages
    # all packages loaded
    assert all(package.state == "loaded" for package in info.load_packages) is True
    # no failed jobs in any of the packages
    info.raise_on_failed_jobs()


def load_table_counts(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    """Returns row counts for `table_names` as dict"""
    with p.sql_client() as c:
        query = "\nUNION ALL\n".join(
            [
                f"SELECT '{name}' as name, COUNT(1) as c FROM {c.make_qualified_table_name(name)}"
                for name in table_names
            ]
        )
        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}


def load_table_distinct_counts(
    p: dlt.Pipeline, distinct_column: str, *table_names: str
) -> DictStrAny:
    """Returns counts of distinct values for column `distinct_column` for `table_names` as dict"""
    with p.sql_client() as c:
        query = "\nUNION ALL\n".join(
            [
                f"SELECT '{name}' as name, COUNT(DISTINCT {distinct_column}) as c FROM {c.make_qualified_table_name(name)}"
                for name in table_names
            ]
        )

        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}


def select_data(
    p: dlt.Pipeline, sql: str, schema_name: str = None
) -> List[Sequence[Any]]:
    """Returns select `sql` results as list."""
    with p.sql_client(schema_name=schema_name) as c:
        with c.execute_query(sql) as cur:
            return list(cur.fetchall())


def get_table_metrics(
    extract_info: ExtractInfo, table_name: str
) -> Optional[Dict[str, Any]]:
    """Returns table metrics from ExtractInfo object."""
    table_metrics_list = [
        d
        for d in extract_info.asdict()["table_metrics"]
        if d["table_name"] == table_name
    ]
    return None if len(table_metrics_list) == 0 else table_metrics_list[0]


def load_data_table_counts(p: dlt.Pipeline) -> DictStrAny:
    """Returns counts for all the data tables in default schema of `p` (excluding dlt tables)"""
    tables = [table["name"] for table in p.default_schema.data_tables()]
    return load_table_counts(p, *tables)


def load_tables_to_dicts(
    p: dlt.Pipeline, *table_names: str
) -> Dict[str, List[Dict[str, Any]]]:
    """Loads tables as rows into lists of python dicts"""
    result = {}
    for table_name in table_names:
        table_rows = []
        columns = p.default_schema.get_table_columns(table_name).keys()

        with p.sql_client() as c:
            query_columns = ",".join(map(c.escape_column_name, columns))
            f_q_table_name = c.make_qualified_table_name(table_name)
            query = f"SELECT {query_columns} FROM {f_q_table_name}"
            with c.execute_query(query) as cur:
                for row in list(cur.fetchall()):
                    table_rows.append(dict(zip(columns, row)))
        result[table_name] = table_rows
    return result


def assert_schema_on_data(
    table_schema: TTableSchema,
    rows: List[Dict[str, Any]],
    requires_nulls: bool,
    check_json: bool,
) -> None:
    """Asserts that `rows` conform to `table_schema`. Fields and their order must conform to columns. Null values and
    python data types are checked.
    """
    table_columns = table_schema["columns"]
    columns_with_nulls: Set[str] = set()
    for row in rows:
        # check columns
        assert set(table_schema["columns"].keys()) == set(row.keys())
        # check column order
        assert list(table_schema["columns"].keys()) == list(row.keys())
        # check data types
        for key, value in row.items():
            if value is None:
                assert table_columns[key][
                    "nullable"
                ], f"column {key} must be nullable: value is None"
                # next value. we cannot validate data type
                columns_with_nulls.add(key)
                continue
            expected_dt = table_columns[key]["data_type"]
            # allow json strings
            if expected_dt == "json":
                if check_json:
                    # NOTE: we expect a dict or a list here. simple types of null will fail the test
                    value = json.loads(value)
                else:
                    # skip checking json types
                    continue
            actual_dt = py_type_to_sc_type(type(value))
            assert actual_dt == expected_dt

    if requires_nulls:
        # make sure that all nullable columns in table received nulls
        assert (
            set(col["name"] for col in table_columns.values() if col["nullable"])
            == columns_with_nulls
        ), "Some columns didn't receive NULLs which is required"


def data_item_length(data: TDataItem) -> int:
    import pandas as pd
    from dlt.common.libs.pyarrow import pyarrow as pa

    if isinstance(data, list):
        # If data is a list, check if it's a list of supported data types
        if all(
            isinstance(item, (list, pd.DataFrame, pa.Table, pa.RecordBatch))
            for item in data
        ):
            return sum(data_item_length(item) for item in data)
        # If it's a list but not a list of supported types, treat it as a single list object
        else:
            return len(data)
    elif isinstance(data, pd.DataFrame):
        return len(data.index)
    elif isinstance(data, pa.Table) or isinstance(data, pa.RecordBatch):
        return data.num_rows
    else:
        raise TypeError("Unsupported data type.")
