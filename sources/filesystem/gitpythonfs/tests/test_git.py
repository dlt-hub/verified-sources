import pytest
from .test_core import repo_fixture

from git import Repo
from gitpythonfs.git_cmd import (
    get_revisions_raw,
    get_revisions_all_raw,
    parse_git_revlist,
)


def test_get_revisions_all_raw(repo_fixture) -> None:
    """Test getting all revisions."""
    d, _ = repo_fixture
    repo = Repo(d)

    result = get_revisions_all_raw(repo, "HEAD")

    assert not result == "", "Should return some info"
    assert all(
        x in result for x in ["file1", "file2", "inner/file3", "inner/file4"]
    ), "Should return info for all files in repo"


def test_get_revisions_all_raw_at_ref(repo_fixture) -> None:
    """Test getting all revisions at ref."""
    d, sha_first = repo_fixture
    repo = Repo(d)

    result = get_revisions_all_raw(repo, sha_first)

    assert "file1" in result, "Should return info for one file that exists at ref"
    assert not "file2" in result, "Should not return info for file not existent at ref"


def test_get_revisions_raw_at_path(repo_fixture) -> None:
    """Test getting revisions at path."""
    d, _ = repo_fixture
    repo = Repo(d)

    result = get_revisions_raw(repo, "HEAD", "inner", "directory")

    assert not result == "", "Should return some info"
    assert all(
        x in result for x in ["inner/file3", "inner/file4"]
    ), "Should return info for all files in `inner` directory"


def test_get_revisions_raw_at_ref(repo_fixture) -> None:
    """Test getting revisions at a ref."""
    d, _ = repo_fixture
    repo = Repo(d)

    result = get_revisions_raw(repo, "thetag", "", "directory")

    assert "file1" in result, "Should return info for one file that exists at ref"
    assert not "file2" in result, "Should not return info for file not existent at ref"


def test_get_revisions_raw_one_file_at_root(repo_fixture) -> None:
    """Test getting revisions for a single file at the root."""
    d, sha_first = repo_fixture
    repo = Repo(d)

    result = get_revisions_raw(repo, "master", "file1", "file")

    assert "file1" in result, "Should return info for one file at root of repo"
    assert not "file2" in result, "Should not return info for this other file"


def test_get_revisions_raw_at_root(repo_fixture) -> None:
    """Test getting revisions at root directory."""
    d, sha_first = repo_fixture
    repo = Repo(d)

    result = get_revisions_raw(repo, "HEAD", "", "directory")

    assert not result == "", "Should return some info"
    assert all(
        x in result for x in ["file1", "file2"]
    ), "Should return info for all files at root of repo"


def test_parse_git_revlist() -> None:
    """Test parsing git revlist output."""

    git_cmd_output = (
        # for clarity, broken into neighbouring constants, which Python
        # automatically concatenates again to be like the original.
        "1703550238\n"
        "\n"
        ":000000 100644 0000000 cdca2c1 A\tinner/file3\n"
        ":000000 100644 0000000 488d13d A\tinner/file4\n"
        "1703550237\n\n:000000 100644 0000000 aff88ce A\tfile2\n"
        "1703550236\n"
        "\n"
        ":100644 100644 1c37a15 a906852 M\tfile1\n"
        "1703550235\n"
        "\n"
        ":000000 100644 0000000 1c37a15 A\tfile1"
    )

    result = parse_git_revlist(git_cmd_output)
    assert result == {
        "inner/file3": 1703550238,
        "inner/file4": 1703550238,
        "file2": 1703550237,
        "file1": 1703550236,
    }, "Should return dict with latest timetamp for each file"
