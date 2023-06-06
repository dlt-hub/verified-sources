import pytest
import os
import sys
from typing import Any, List

from dlt.common.configuration.providers import SecretsTomlProvider
from dlt.common.storages.file_storage import FileStorage
from dlt.extract.decorators import _SOURCES
from dlt.common.utils import set_working_dir

from dlt.cli import init_command, echo
from dlt.cli.init_command import PIPELINES_MODULE_NAME, utils as cli_utils, files_ops
from dlt.reflection import names as n

from tests.utils import TEST_STORAGE_ROOT

# todo change in core
PIPELINES_MODULE_NAME = "sources"

INIT_REPO_LOCATION = os.path.abspath(".")  # scan this very repo
PROJECT_DIR = os.path.join(TEST_STORAGE_ROOT, "project")


@pytest.fixture(autouse=True)
def echo_default_choice() -> None:
    """Always answer default in CLI interactions"""
    echo.ALWAYS_CHOOSE_DEFAULT = True
    yield
    echo.ALWAYS_CHOOSE_DEFAULT = False


@pytest.fixture(autouse=True)
def unload_modules() -> None:
    """Unload all modules inspected in this tests"""
    prev_modules = dict(sys.modules)
    yield
    mod_diff = set(sys.modules.keys()) - set(prev_modules.keys())
    for mod in mod_diff:
        del sys.modules[mod]


def get_pipeline_candidates() -> List[str]:
    """Get all pipelines in `pipelines` folder"""
    pipelines_storage = FileStorage(os.path.join(".", PIPELINES_MODULE_NAME))
    # enumerate all candidate pipelines
    return files_ops.get_pipeline_names(pipelines_storage)


def get_project_files() -> FileStorage:
    _SOURCES.clear()
    # project dir
    return FileStorage(PROJECT_DIR, makedirs=True)


@pytest.mark.parametrize("candidate", get_pipeline_candidates())
def test_init_all_pipelines(candidate: str) -> None:
    files = get_project_files()
    with set_working_dir(files.storage_path):
        init_command.init_command(candidate, "bigquery", False, INIT_REPO_LOCATION)
        assert_pipeline_files(files, candidate, "bigquery")
        assert_requests_txt(files)


def test_init_list_pipelines() -> None:
    pipelines = init_command._list_pipelines(INIT_REPO_LOCATION)
    # a few known pipelines must be there
    assert set(get_pipeline_candidates()) == set(pipelines.keys())
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
        assert args.arguments["destination"].value == destination_name
    # load secrets
    secrets = SecretsTomlProvider()
    if destination_name != "duckdb":
        assert secrets.get_value(destination_name, Any, "destination") is not None
