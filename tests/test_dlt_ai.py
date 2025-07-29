import pytest
from pathlib import Path
from packaging.version import Version

import dlt
from dlt.common.utils import set_working_dir
from dlt.cli import ai_command
from dlt.cli.ai_command import TSupportedIde
from dlt.cli.plugins import DEFAULT_VERIFIED_SOURCES_REPO

from tests.utils import TEST_STORAGE_ROOT

# NOTE: This test is also available in dlt
# We have it here to make sure we update the ai command in a timely manner


@pytest.mark.parametrize(
    ("ide", "expected_relative_path", "is_single_file"),
    (
        ("amp", "AGENT.md", True),
        ("codex", "AGENT.md", True),
        ("claude", "CLAUDE.md", True),
        ("cody", ".sourcegraph", False),
        ("cline", ".clinerules", False),
        ("cursor", ".cursor/rules/", False),
        ("continue", ".continue/rules/", False),
        ("windsurf", ".windsurf/rules/", False),
        ("copilot", ".github/instructions/", False),
    ),
)
def test_ai_setup_command(
    ide: TSupportedIde, expected_relative_path: str, is_single_file: bool
) -> None:
    with set_working_dir(TEST_STORAGE_ROOT):
        if Version(dlt.__version__) < Version("1.12.3"):
            ai_command.ai_setup_command(
                ide=ide,
                repo=DEFAULT_VERIFIED_SOURCES_REPO,
                branch="feat/continue-rules",
            )
        else:
            ai_command.ai_setup_command(
                ide=ide,
                location=DEFAULT_VERIFIED_SOURCES_REPO,
                branch="feat/continue-rules",
            )

    base_path = Path(TEST_STORAGE_ROOT).resolve()
    expected_location = base_path / expected_relative_path

    if is_single_file:
        assert expected_location.is_file()
        return

    assert expected_location.is_dir()
    all_files = list(expected_location.rglob("*"))
    if ide == "cursor":
        assert all(file.suffix == ".mdc" for file in all_files)
        assert (base_path / ".cursorignore").is_file()
    else:
        assert all(file.suffix == ".md" for file in all_files)
