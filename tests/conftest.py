import os
from typing import List

# will run the following auto use fixtures for all tests ie. to cleanup the datasets, env variables and file system after each test
from tests.utils import (
    drop_pipeline,
    test_config_providers,
    patch_pipeline_working_dir,
    new_test_storage,
    preserve_environ,
)

# will force duckdb to be created in pipeline folder
from dlt.destinations.duckdb.configuration import DuckDbCredentials

DuckDbCredentials.database = ":pipeline:"


def pytest_configure(config):
    # patch which providers to enable
    # from dlt.common.configuration.providers import ConfigProvider, EnvironProvider, SecretsTomlProvider, ConfigTomlProvider
    # from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext

    # def initial_providers() -> List[ConfigProvider]:
    #     # do not read the global config
    #     return [EnvironProvider(), SecretsTomlProvider(add_global_config=False), ConfigTomlProvider(add_global_config=False)]

    # ConfigProvidersContext.initial_providers = initial_providers

    # push telemetry to CI
    os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "False"
    os.environ[
        "RUNTIME__DLTHUB_TELEMETRY_SEGMENT_WRITE_KEY"
    ] = "TLJiyRkGVZGCi2TtjClamXpFcxAA1rSB"

    # path pipeline instance id up to millisecond
    from dlt.common import pendulum
    from dlt.pipeline.pipeline import Pipeline

    def _create_pipeline_instance_id(self) -> str:
        return pendulum.now().format("_YYYYMMDDhhmmssSSSS")

    Pipeline._create_pipeline_instance_id = _create_pipeline_instance_id
