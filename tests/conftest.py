# will run the following auto use fixtures for all tests ie. to cleanup the datasets, env variables and file system after each test
from tests.utils import drop_pipeline, test_config_providers, patch_pipeline_working_dir, new_test_storage, preserve_environ

# will force duckdb to be created in pipeline folder
from dlt.destinations.duckdb.configuration import DuckDbCredentials
DuckDbCredentials.database = ":pipeline:"
