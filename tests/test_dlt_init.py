import pytest
import os
from typing import Any, Iterator, List

from dlt.common.configuration.providers import SecretsTomlProvider
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import set_working_dir

from dlt.sources import SourceReference

from dlt.cli import init_command, echo
from dlt.cli.init_command import SOURCES_MODULE_NAME, utils as cli_utils, files_ops
from dlt.reflection import names as n

from tests.utils import TEST_STORAGE_ROOT

# todo change in core

INIT_REPO_LOCATION = os.path.abspath(".")  # scan this very repo
PROJECT_DIR = os.path.join(TEST_STORAGE_ROOT, "project")
CORE_SOURCES = {"filesystem", "rest_api", "sql_database"}


@pytest.fixture(autouse=True)
def echo_default_choice() -> Iterator[None]:
    """Always answer default in CLI interactions"""
    echo.ALWAYS_CHOOSE_DEFAULT = True
    yield
    echo.ALWAYS_CHOOSE_DEFAULT = False


# @pytest.fixture(autouse=True)
# def unload_modules() -> None:
#     """Unload all modules inspected in this tests"""
#     prev_modules = dict(sys.modules)
#     yield
#     mod_diff = set(sys.modules.keys()) - set(prev_modules.keys())
#     for mod in mod_diff:
#         del sys.modules[mod]


def get_pipeline_candidates() -> List[str]:
    """Get all pipelines in `sources` folder"""
    pipelines_storage = FileStorage(os.path.join(".", SOURCES_MODULE_NAME))
    # enumerate all candidate pipelines
    return files_ops.get_sources_names(pipelines_storage, "verified")


PIPELINE_CANDIDATES = set(get_pipeline_candidates())


def get_project_files() -> FileStorage:
    SourceReference.SOURCES.clear()
    # project dir
    return FileStorage(PROJECT_DIR, makedirs=True)


@pytest.mark.parametrize("candidate", PIPELINE_CANDIDATES - CORE_SOURCES)
def test_init_all_pipelines(candidate: str) -> None:
    files = get_project_files()
    with set_working_dir(files.storage_path):
        init_command.init_command(candidate, "bigquery", INIT_REPO_LOCATION)
        assert_pipeline_files(files, candidate, "bigquery")
        assert_requests_txt(files)


def test_init_list_pipelines() -> None:
    pipelines = init_command._list_verified_sources(INIT_REPO_LOCATION)
    # a few known pipelines must be there
    assert PIPELINE_CANDIDATES == set(pipelines.keys())
    # check docstrings
    for k_p in pipelines:
        assert pipelines[
            k_p
        ].doc, f"Please provide module docstring in the __init__.py of {k_p} pipeline"


def assert_requests_txt(project_files: FileStorage) -> None:
    # check requirements
    assert project_files.has_file(cli_utils.REQUIREMENTS_TXT)
    assert "dlt" in project_files.load(cli_utils.REQUIREMENTS_TXT)


def assert_pipeline_files(
    project_files: FileStorage, pipeline_name: str, destination_name: str
) -> None:
    # inspect script
    pipeline_script = pipeline_name + "_pipeline.py"
    visitor = cli_utils.parse_init_script(
        "test", project_files.load(pipeline_script), pipeline_script
    )
    # check destinations
    for args in visitor.known_calls[n.PIPELINE]:
        destination = args.arguments["destination"]
        if pipeline_name == "pg_replication":
            # allow None as `pg_replication` uses a workaround in its example
            # pipeline to fix the destination such that it does not get replaced
            # during `dlt init`
            assert destination is None or destination.value == destination_name
        else:
            assert destination.value == destination_name
    # load secrets
    secrets = SecretsTomlProvider(settings_dir=".dlt")
    if destination_name != "duckdb":
        assert secrets.get_value(destination_name, Any, "destination") is not None
