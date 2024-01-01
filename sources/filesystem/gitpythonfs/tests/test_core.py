import os
import subprocess
import tempfile
import shutil

import pytest
from typing import Iterator

import fsspec  # ToDo, narrow for open()
from fsspec.implementations.local import make_path_posix
from fsspec.registry import (
    get_filesystem_class,
    known_implementations,
    available_protocols,
    filesystem,
)

from gitpythonfs import GitPythonFileSystem
from gitpythonfs.core import register_implementation_in_fsspec

PROTOCOL = "gitpythonfs"
# TEST_REPO_PATH = ["~/dlt"]


@pytest.fixture()
def repo_fixture() -> Iterator[tuple[str, str]]:
    """Create a temporary git repository.

    Thanks to https://github.com/fsspec/filesystem_spec/blob/master/fsspec/implementations/tests/test_git.py
    """
    orig_dir = os.getcwd()
    d = tempfile.mkdtemp()
    try:
        os.chdir(d)
        # -b option requires git 2.28 or later.
        subprocess.call("git init -b master", shell=True, cwd=d)
        subprocess.call('git config user.email "you@example.com"', shell=True, cwd=d)
        subprocess.call('git config user.name "Your Name"', shell=True, cwd=d)
        open(os.path.join(d, "file1"), "wb").write(b"data0")
        subprocess.call("git add file1", shell=True, cwd=d)
        subprocess.call('git commit -m "init"', shell=True, cwd=d)
        sha_first = open(os.path.join(d, ".git/refs/heads/master"), "r").read().strip()
        open(os.path.join(d, "file1"), "wb").write(b"data00")
        subprocess.check_output('git commit -a -m "tagger"', shell=True, cwd=d)
        subprocess.call('git tag -a thetag -m "make tag"', shell=True, cwd=d)
        open(os.path.join(d, "file2"), "wb").write(b"data000")
        subprocess.call("git add file2", shell=True)
        subprocess.call('git commit -m "master tip"', shell=True, cwd=d)
        subprocess.call("git checkout -b abranch", shell=True, cwd=d)
        os.mkdir("inner")
        open(os.path.join(d, "inner", "file3"), "wb").write(b"data3")
        subprocess.call("git add inner/file3", shell=True, cwd=d)
        open(os.path.join(d, "inner", "file4"), "wb").write(b"data4")
        subprocess.call("git add inner/file4", shell=True, cwd=d)
        subprocess.call('git commit -m "branch tip"', shell=True, cwd=d)
        os.chdir(orig_dir)
        yield d, sha_first
    finally:
        os.chdir(orig_dir)
        shutil.rmtree(d)


def test_register_implementation_in_fsspec() -> None:
    """Test registering a filesystem with fsspec."""
    known_implementations.pop(PROTOCOL)
    assert (
        not PROTOCOL in known_implementations
    ), f"As a test precondition, {PROTOCOL} should not be registered."

    register_implementation_in_fsspec()
    assert PROTOCOL in available_protocols(), f"{PROTOCOL} should be registered."

    cls = get_filesystem_class(PROTOCOL)
    assert cls == GitPythonFileSystem


# @pytest.mark.parametrize("path", TEST_REPO_PATH)
def test_instantiate_fsspec_filesystem(repo_fixture) -> None:
    """Test instantiating a filesystem with fsspec.

    Args:
        repo_path (str): The path to the repository. The repository must exist.
    """
    d = repo_fixture[0]

    fs = filesystem(PROTOCOL, path=d)
    assert type(fs) == GitPythonFileSystem


def test_ls_entries(repo_fixture):
    """Test listing folders and files in a repository."""
    d, sha_first = repo_fixture
    fs = filesystem(PROTOCOL, path=d)

    assert fs.ls("") == [
        "file1",
        "file2",
        "inner",
    ], "Should return all objects at root of repo."
    assert fs.ls("file1") == ["file1"], "Should return a single file at root."
    assert fs.ls("inner") == [
        "inner/file3",
        "inner/file4",
    ], "Should return 2 files, with their paths."
    assert fs.ls("inner/file3") == [
        "inner/file3"
    ], "Should return a single file in folder."


def test_ls_file_details(repo_fixture) -> None:
    """Test showing details for a file (git.Blob) in a repository."""

    # setup
    d, sha_first = repo_fixture
    fs = filesystem(PROTOCOL, path=d)

    # do
    files = fs.ls("file1", detail=True, include_committed_date=True)
    assert len(files) == 1, "Should return a single object."
    details = files[0]

    # assert
    assert details["name"] == "file1"
    assert details["type"] == "file"
    assert details["mime_type"] == "text/plain"
    assert isinstance(details["size"], int)
    assert isinstance(details["hex"], str)
    assert isinstance(
        details["mode"], str
    ), "Should be a string representation of octal, without the 0o prefix."
    assert isinstance(details["committed_date"], int)


def test_git_refs(repo_fixture) -> None:
    """Test results for git refs - eg sha, branch, tag."""
    d, sha_first = repo_fixture

    with fsspec.open("gitpythonfs://inner/file3", path=d) as f:
        bytes = f.read()
        assert bytes == b"data3", "Should read from head if no ref given."

    # with fsspec.open("gitpythonfs://file1", path=d, ref=sha_first) as f:
    #     bytes = f.read()
    #     assert bytes == b"data0", "Should read file version at given ref."

    # with fsspec.open("git://file1", path=d, ref="thetag") as f:
    #     assert f.read() == b"data00"

    # with fsspec.open("git://file2", path=d, ref="master") as f:
    #     assert f.read() == b"data000"

    # with fsspec.open("git://file2", path=d, ref=None) as f:
    #     assert f.read() == b"data000"

    # with fsspec.open("git://inner/file1", path=d, ref="abranch") as f:
    #     assert f.read() == b"data3"


def test_url(repo_fixture) -> None:
    d, sha_first = repo_fixture

    with fsspec.open(f"gitpythonfs://file1", path=d) as f:
        assert f.read() == b"data00", "Should return file at root."

    # ToDo: implement/test more complex urls, eg gitpythonfs://[path-to-repo[:]][ref@]path/to/file as used in git fsspec implementation.
    #   The colon (:) is compulsory if path-to-repo is give. ie, gitpythonfs://[path-to-repo:][ref@]path/to/file

    with fsspec.open(f"gitpythonfs://{d}:file1") as f:
        assert (
            f.read() == b"data00"
        ), "Should return file via the repo path embedded in the url."

    # ToDo expand test once refs supported.
    # with fsspec.open(f"gitpythonfs://abranch@inner/file3", path=d) as f:
    #     assert f.read() == b"data3", "Should return file at ref embedded in url."
    # with fsspec.open(f"gitpythonfs://{d}:abranch@innerfile3") as f:
    #     assert f.read() == b"data3", "Should return file at repo and ref embedded in url."


def test_multiple_files(repo_fixture) -> None:
    """Test reading multiple files from a repository."""
    d, sha_first = repo_fixture

    files = fsspec.open_files(f"gitpythonfs://{d}:**/file*")
    assert (
        len(files) == 4
    ), "Glob should recurse folders and return 4 files that start with `file`."


def test_non_readonly_raises_exception(repo_fixture) -> None:
    """Test that non-readonly operations raise an exception."""
    d, _ = repo_fixture

    with pytest.raises(NotImplementedError):
        GitPythonFileSystem(d).mv()
