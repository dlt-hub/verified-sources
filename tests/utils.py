import pytest
from typing import Any, Iterator, List
from os import environ
from unittest.mock import patch

import dlt

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.configuration.providers import EnvironProvider, ConfigTomlProvider, SecretsTomlProvider
from dlt.common.pipeline import LoadInfo, PipelineContext
from dlt.common.storages import FileStorage

from dlt.pipeline.exceptions import SqlClientNotAvailable

from tests.sql_source import SQLAlchemySourceDB

TEST_STORAGE_ROOT = "_storage"
ALL_DESTINATIONS = ["bigquery", "redshift", "postgres"]
# ALL_DESTINATIONS = ['postgres', 'bigquery']
# ALL_DESTINATIONS = ["postgres"]
# ALL_DESTINATIONS = ["bigquery"]


@pytest.fixture(autouse=True)
def drop_pipeline() -> Iterator[None]:
    """Drops all the datasets for the pipeline created in the test. Does not drop the working dir - see new_test_storage"""
    yield
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

        # deactivate context
        Container()[PipelineContext].deactivate()


@pytest.fixture(autouse=True)
def test_config_providers() -> Iterator[ConfigProvidersContext]:
    """Creates set of config providers where tomls are loaded from tests/.dlt"""
    config_root = "./pipelines/.dlt"
    ctx = ConfigProvidersContext()
    ctx.providers.clear()
    ctx.add_provider(EnvironProvider())
    ctx.add_provider(SecretsTomlProvider(project_dir=config_root))
    ctx.add_provider(ConfigTomlProvider(project_dir=config_root))
    # replace in container
    Container()[ConfigProvidersContext] = ctx


@pytest.fixture(autouse=True)
def patch_pipeline_working_dir() -> None:
    """Puts the pipeline working directory into test storage"""
    with patch("dlt.common.pipeline._get_home_dir") as _get_home_dir:
        _get_home_dir.return_value = TEST_STORAGE_ROOT
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


@pytest.fixture(scope='session')
def sql_source_db():
    db = SQLAlchemySourceDB()
    try:
        db.create_schema()
        db.create_tables()
        db.insert_data()
        yield db
    finally:
        db.drop_schema()



def clean_test_storage(init_normalize: bool = False, init_loader: bool = False, mode: str = "t") -> FileStorage:
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


def assert_table_data(p: dlt.Pipeline, table_name: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    assert_query_data(p, f"SELECT * FROM {table_name} ORDER BY 1 NULLS FIRST", table_data, schema_name, info)


def assert_query_data(p: dlt.Pipeline, sql: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
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
    assert len(info.loads_ids) == expected_load_packages
    # all packages loaded
    assert all(info.loads_ids.values()) is True
    # no failed jobs in any of the packages
    assert all(len(jobs) == 0 for jobs in info.failed_jobs.values()) is True
