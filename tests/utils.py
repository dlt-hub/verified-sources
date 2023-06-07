import os
import pytest
from typing import Any, Iterator, List
from os import environ
from unittest.mock import patch

import dlt
from dlt.common.typing import DictStrAny
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)
from dlt.common.configuration.providers import (
    EnvironProvider,
    ConfigTomlProvider,
    SecretsTomlProvider,
)
from dlt.common.pipeline import LoadInfo, PipelineContext
from dlt.common.storages import FileStorage

from dlt.pipeline.exceptions import SqlClientNotAvailable

TEST_STORAGE_ROOT = "_storage"

# get env variable with destinations
ALL_DESTINATIONS = dlt.config.get("ALL_DESTINATIONS", list) or [
    "duckdb",
]
# ALL_DESTINATIONS = ["duckdb"]
# ALL_DESTINATIONS = ["bigquery"]


@pytest.fixture(autouse=True)
def drop_pipeline() -> Iterator[None]:
    """Drops all the datasets for the pipeline created in the test. Does not drop the working dir - see new_test_storage"""
    yield
    drop_active_pipeline_data()


@pytest.fixture(autouse=True, scope="session")
def test_config_providers() -> Iterator[ConfigProvidersContext]:
    """Creates set of config providers where tomls are loaded from tests/.dlt"""
    config_root = "./sources/.dlt"
    ctx = ConfigProvidersContext()
    ctx.providers.clear()
    ctx.add_provider(EnvironProvider())
    ctx.add_provider(
        SecretsTomlProvider(project_dir=config_root, add_global_config=False)
    )
    ctx.add_provider(
        ConfigTomlProvider(project_dir=config_root, add_global_config=False)
    )
    # replace in container
    Container()[ConfigProvidersContext] = ctx
    # extras work when container updated
    ctx.add_extras()


@pytest.fixture(autouse=True)
def patch_pipeline_working_dir() -> None:
    """Puts the pipeline working directory into test storage"""
    try:
        with patch(
            "dlt.common.configuration.paths._get_user_home_dir"
        ) as _get_home_dir:
            _get_home_dir.return_value = os.path.abspath(TEST_STORAGE_ROOT)
            yield
    except ModuleNotFoundError:
        with patch("dlt.common.pipeline._get_home_dir") as _get_home_dir:
            _get_home_dir.return_value = os.path.abspath(TEST_STORAGE_ROOT)
            yield


@pytest.fixture(autouse=True)
def new_test_storage() -> FileStorage:
    """Creates new empty test storage in ./_storage folder"""
    return clean_test_storage()


@pytest.fixture(scope="function", autouse=True)
def preserve_environ() -> None:
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
                    with c.with_staging_dataset(staging=True):
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

        LoadStorage(True, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
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
    query = "\nUNION ALL\n".join(
        [f"SELECT '{name}' as name, COUNT(1) as c FROM {name}" for name in table_names]
    )
    with p.sql_client() as c:
        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}


def load_table_distinct_counts(
    p: dlt.Pipeline, distinct_column: str, *table_names: str
) -> DictStrAny:
    """Returns counts of distinct values for column `distinct_column` for `table_names` as dict"""
    query = "\nUNION ALL\n".join(
        [
            f"SELECT '{name}' as name, COUNT(DISTINCT {distinct_column}) as c FROM {name}"
            for name in table_names
        ]
    )
    with p.sql_client() as c:
        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}


# def assert_tables_filled()
