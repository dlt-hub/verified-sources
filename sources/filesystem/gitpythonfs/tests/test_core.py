import os
import subprocess
import tempfile
import shutil

import pytest
from typing import Iterator

import fsspec

from fsspec.registry import (
    get_filesystem_class,
    known_implementations,
    available_protocols,
    filesystem,
)

from git import BadName

from gitpythonfs import GitPythonFileSystem
from gitpythonfs.core import register_implementation_in_fsspec

PROTOCOL = GitPythonFileSystem.PROTOCOL


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


def test_instantiate_fsspec_filesystem(repo_fixture) -> None:
    """Test instantiating a filesystem with fsspec."""
    d, _ = repo_fixture

    fs = filesystem(PROTOCOL, repo_path=d)
    assert type(fs) == GitPythonFileSystem


def test_ls_entries(repo_fixture):
    """Test listing folders and files in a repository."""
    d, sha_first = repo_fixture
    fs = filesystem(PROTOCOL, repo_path=d)

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
    fs = filesystem(PROTOCOL, repo_path=d)

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
    """Test results for git refs - eg commit sha, branch, tag."""
    d, _ = repo_fixture

    with fsspec.open("gitpythonfs://inner/file3", repo_path=d) as f:
        assert f.read() == b"data3", "Should read from HEAD if no ref given."

    try:
        with fsspec.open("gitpythonfs://inner/file3", repo_path=d, ref="HEAD") as f:
            f.read()
    except BadName:
        pytest.fail("Should accept HEAD as a ref.")

    with pytest.raises(BadName):
        with fsspec.open(
            "gitpythonfs://file1", repo_path=d, ref="somenonexisentgitref"
        ) as f:
            _ = f.read()


def test_git_refs_on_open(repo_fixture) -> None:
    d, sha_first = repo_fixture

    with fsspec.open("gitpythonfs://file1", repo_path=d, ref=sha_first) as f:
        assert (
            f.read() == b"data0"
        ), "Should read file version at given sha (aka commit id)."

    with fsspec.open("gitpythonfs://file1", repo_path=d, ref="thetag") as f:
        assert f.read() == b"data00", "Should read file version at given tag."


def test_git_refs_on_ls(repo_fixture) -> None:
    d, sha_first = repo_fixture

    fs = filesystem(PROTOCOL, repo_path=d, ref=sha_first)
    files = fs.ls("file1", detail=True)
    assert len(files) == 1, "Should return a single object."
    assert files[0]["size"] == 5, "Should return file size as at sha given in __init__."

    fs = filesystem(PROTOCOL, repo_path=d)
    files = fs.ls("file1", ref=sha_first, detail=True)
    assert len(files) == 1, "Should return a single object."
    assert files[0]["size"] == 5, "Should return file size as at sha given in ls()."

    fs = filesystem(PROTOCOL, repo_path=d, ref="HEAD")
    files = fs.ls("file1", ref=sha_first, detail=True)
    assert len(files) == 1, "Should return a single object."
    assert files[0]["size"] == 5, "ls() ref should override constructor ref."


def test_get_kwargs_from_urls() -> None:
    """Test getting kwargs from url."""
    repo_path = "/some/path/to/repo"
    ref = "some_tag"

    url = f"gitpythonfs://{repo_path}:{ref}@file1"
    kwargs = GitPythonFileSystem._get_kwargs_from_urls(url)
    assert kwargs["repo_path"] == repo_path
    assert kwargs["ref"] == ref


def test_url(repo_fixture) -> None:
    """Test reading a file from a repository via url.

    For supported url formats see GitPytonFileSystem class doco"""

    d, sha_first = repo_fixture

    with fsspec.open(f"gitpythonfs://file1", repo_path=d) as f:
        assert f.read() == b"data00", "Should return file at root."

    with fsspec.open(f"gitpythonfs://{d}:file1") as f:
        assert (
            f.read() == b"data00"
        ), "Should return file via the repo path embedded in the url."

    with fsspec.open(f"gitpythonfs://{d}:{sha_first}@file1") as f:
        assert (
            f.read() == b"data0"
        ), "Should return file via the repo path and git ref embedded in the url."


@pytest.mark.skip(
    reason="Supplying arg in both url and function call throws fsspec error. Works in other fsspec implementatins?"
)
def test_args_precedence(repo_fixture) -> None:
    """Test precedence of arguments passed in function parameters
    vs passed in url."""
    d, sha_first = repo_fixture

    with fsspec.open(f"gitpythonfs://{d}:HEAD@file1", ref=sha_first) as f:
        assert f.read() == b"data0", "Should return file for ref supplied in ???."


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
