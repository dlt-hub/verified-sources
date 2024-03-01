from typing import Union, Any, Iterator

import pytest

import os
import subprocess
import shutil
import pathlib
from .settings import (
    TEST_SAMPLES_PATH,
    REPO_FIXTURE_PATH,
    REPO_SAFE_PREFIX,
    REPO_GOOD_REF,
)


@pytest.fixture(scope="module", autouse=True)
def repo_fixture(
    repo_path: Union[str, os.PathLike] = REPO_FIXTURE_PATH,
    samples_path: Union[str, os.PathLike] = TEST_SAMPLES_PATH,
    safe_prefix: str = REPO_SAFE_PREFIX,
    good_ref: str = REPO_GOOD_REF,
) -> Iterator[Any]:
    """Create a temporary git repository to test git-based filesystems.

    Args:
        repo_path (str): The path at which to create the temporary repo. Defaults to REPO_FIXTURE_PATH. It is safest
            to create the repo outside your software project's directory tree so it does not interfere with real repos.
        samples_path (str): The path to the sample files, which will be added to the repo. Defaults to TEST_SAMPLES_PATH.
        safe_prefix (str): Helps prevent mixups between the test repo and real repos. Defaults to REPO_SAFE_PREFIX.
        good_ref (str): The git ref for the unmodified sample files. Later commits intentionally break the sample
            files so that tests will fail if the system under test doesn't correctly handle refs.
            Defaults to REPO_SAFE_REF.

    Yields:
        Tuple[str, str]: A tuple containing the repo_path and good_ref.

    """
    repo_path = pathlib.Path(repo_path)
    samples_path = pathlib.Path(samples_path)

    try:
        try:
            os.mkdir(repo_path)
        except FileExistsError:
            raise FileExistsError(
                f"Directory `{repo_path.absolute()}` already exists."
                "It should have been removed by the previous test run."
            )

        # NOTE: `git init -b` option requires git 2.28 or later.
        subprocess.call(f"git init -b {safe_prefix}master", shell=True, cwd=repo_path)
        subprocess.call(
            "git config user.email 'you@example.com'", shell=True, cwd=repo_path
        )
        subprocess.call("git config user.name 'Your Name'", shell=True, cwd=repo_path)
        shutil.copytree(
            samples_path, repo_path / f"{safe_prefix}samples", dirs_exist_ok=False
        )
        subprocess.call("git add --all", shell=True, cwd=repo_path)
        subprocess.call(
            "git commit -m 'add standard sample files for tests'",
            shell=True,
            cwd=repo_path,
        )
        subprocess.call(
            f"git tag -a {good_ref} -m 'The sample test files with no modifications'",
            shell=True,
            cwd=repo_path,
        )
        subprocess.call(
            "git mv sample.txt sample_renamed.txt",
            shell=True,
            cwd=repo_path / f"{safe_prefix}samples",
        )
        subprocess.call(
            "git commit -m 'rename samples.txt to make primitive test fail if at HEAD'",
            shell=True,
            cwd=repo_path,
        )
        yield repo_path, good_ref
    finally:
        shutil.rmtree(repo_path)
