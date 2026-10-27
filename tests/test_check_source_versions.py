import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest

from tools.check_source_versions import has_changelog_entry, main, parse_version

README_1_0_0 = "# A\n\n## Changelog\n\n### 1.0.0\n- first version\n"
README_1_1_0 = (
    "# A\n\n## Changelog\n\n### 1.1.0\n- new resource\n\n### 1.0.0\n- first version\n"
)


def git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


def write(repo: Path, files: Dict[str, Optional[str]]) -> None:
    for path, content in files.items():
        if content is None:
            (repo / path).unlink()
        else:
            (repo / path).parent.mkdir(parents=True, exist_ok=True)
            (repo / path).write_text(content, encoding="utf-8")


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A repository with a versioned source `a` and an unversioned source `b` on master, and a
    feature branch checked out
    """
    git(tmp_path, "init", "-b", "master")
    git(tmp_path, "config", "user.email", "dev@example.com")
    git(tmp_path, "config", "user.name", "dev")
    write(
        tmp_path,
        {
            "sources/a/__init__.py": "",
            "sources/a/version.py": '__version__ = "1.0.0"\n',
            "sources/a/README.md": README_1_0_0,
            "sources/a_pipeline.py": "",
            "sources/b/__init__.py": "",
            "sources/b/README.md": "# B\n",
            "tests/a/test_a.py": "",
        },
    )
    git(tmp_path, "add", "-A")
    git(tmp_path, "commit", "-m", "init")
    git(tmp_path, "checkout", "-b", "feature")
    monkeypatch.setenv("SOURCES_BASE_REF", "master")
    monkeypatch.delenv("GITHUB_BASE_REF", raising=False)
    return tmp_path


@pytest.mark.parametrize(
    "files,commit,expected_errors",
    [
        ({}, True, []),
        (
            {"sources/a/__init__.py": "x = 1\n"},
            False,
            ["bump the version", "update sources/a/README.md"],
        ),
        (
            {
                "sources/a/__init__.py": "x = 1\n",
                "sources/a/version.py": '__version__ = "1.1.0"\n',
                "sources/a/README.md": README_1_1_0,
            },
            True,
            [],
        ),
        (
            {
                "sources/a/__init__.py": "x = 1\n",
                "sources/a/version.py": '__version__ = "1.1.0"\n',
                "sources/a/README.md": README_1_0_0 + "\nmore notes\n",
            },
            False,
            ["add a changelog entry `### 1.1.0`"],
        ),
        (
            {
                "sources/a/__init__.py": "x = 1\n",
                "sources/a/version.py": '__version__ = "0.9.0"\n',
                "sources/a/README.md": README_1_0_0 + "### 0.9.0\n",
            },
            False,
            ["bump the version"],
        ),
        (
            {
                "sources/a/__init__.py": "x = 1\n",
                "sources/a/version.py": '__version__ = "1.1"\n',
                "sources/a/README.md": README_1_1_0,
            },
            False,
            ["not a MAJOR.MINOR.PATCH string"],
        ),
        ({"sources/a/README.md": README_1_0_0 + "\nmore notes\n"}, True, []),
        (
            {"sources/a_pipeline.py": "x = 1\n", "tests/a/test_a.py": "x = 1\n"},
            True,
            [],
        ),
        (
            {"sources/b/__init__.py": "x = 1\n"},
            True,
            ["add sources/b/version.py", "update sources/b/README.md"],
        ),
        (
            {
                "sources/b/__init__.py": "x = 1\n",
                "sources/b/version.py": '__version__ = "1.0.0"\n',
                "sources/b/README.md": README_1_0_0,
            },
            True,
            [],
        ),
        (
            {
                "sources/c/__init__.py": "",
                "sources/c/version.py": '__version__ = "1.0.0"\n',
            },
            False,
            ["add sources/c/README.md"],
        ),
        (
            {
                "sources/c/__init__.py": "",
                "sources/c/version.py": '__version__ = "1.0.0"\n',
                "sources/c/README.md": README_1_0_0,
            },
            False,
            [],
        ),
        (
            {
                "sources/a/__init__.py": None,
                "sources/a/version.py": None,
                "sources/a/README.md": None,
            },
            True,
            [],
        ),
    ],
    ids=[
        "no_changes",
        "no_bump",
        "bumped",
        "no_changelog_entry",
        "lower_version",
        "invalid_version",
        "readme_only",
        "pipeline_and_tests_only",
        "unversioned_source_changed",
        "unversioned_source_versioned",
        "new_source_without_readme",
        "new_source_uncommitted",
        "removed_source",
    ],
)
def test_check_source_versions(
    repo: Path,
    capsys: pytest.CaptureFixture[str],
    files: Dict[str, Optional[str]],
    commit: bool,
    expected_errors: List[str],
) -> None:
    write(repo, files)
    if commit and files:
        git(repo, "add", "-A")
        git(repo, "commit", "-m", "change")

    exit_code = main(repo)

    output = capsys.readouterr().out
    errors = [line for line in output.splitlines() if line.startswith("ERROR")]
    assert exit_code == (1 if expected_errors else 0)
    assert len(errors) == len(expected_errors)
    for expected in expected_errors:
        assert any(expected in error for error in errors), output


def test_check_source_versions_without_base(
    repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SOURCES_BASE_REF", "missing")
    git(repo, "branch", "-m", "master", "main")
    with pytest.raises(RuntimeError, match="No base branch found"):
        main(repo)


@pytest.mark.parametrize(
    "code,expected",
    [
        ('__version__ = "1.2.3"\n', (1, 2, 3)),
        ('"""doc"""\n\n__version__ = "10.0.12"\n', (10, 0, 12)),
    ],
    ids=["plain", "docstring"],
)
def test_parse_version(code: str, expected: Tuple[int, int, int]) -> None:
    assert parse_version(code) == expected


@pytest.mark.parametrize(
    "code",
    ['__version__ = "1.2"\n', '__version__ = "1.2.3a1"\n', "VERSION = '1.0.0'\n"],
    ids=["two_parts", "pre_release", "other_name"],
)
def test_parse_version_invalid(code: str) -> None:
    with pytest.raises(ValueError):
        parse_version(code)


@pytest.mark.parametrize(
    "readme,expected",
    [
        ("## Changelog\n\n### 1.2.0\n", True),
        ("## Changelog\n\n### v1.2.0 - breaking\n", True),
        ("## Changelog\n\n### 1.2.0.1\n", False),
        ("## Changelog\n\n### 1.2.01\n", False),
        ("bumped to 1.2.0\n", False),
    ],
    ids=[
        "heading",
        "prefixed_heading",
        "longer_version",
        "longer_patch",
        "not_heading",
    ],
)
def test_has_changelog_entry(readme: str, expected: bool) -> None:
    assert has_changelog_entry(readme, "1.2.0") is expected
