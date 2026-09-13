"""Regression tests for https://github.com/dlt-hub/verified-sources/issues/685.

`sources/pg_replication/helpers.py` imports `dlt.sources.sql_database` at module
level, which requires SQLAlchemy to be installed (see
`dlt.common.libs.sql_alchemy`: it raises `MissingDependencyException` without
it). Both dependency manifests of the source -- the `requirements.txt`
consumed by `dlt init` and the `pg_replication` uv dependency group -- must
therefore declare SQLAlchemy. Otherwise a fresh install of just the declared
requirements fails at import time with `MissingDependencyException`, which is
invisible to contributors because the default dev groups pull in the sibling
`sql_database` group anyway.
"""

import re
from pathlib import Path
from typing import Dict

from packaging.requirements import Requirement

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_REQUIREMENTS = REPO_ROOT / "sources" / "pg_replication" / "requirements.txt"
PYPROJECT = REPO_ROOT / "pyproject.toml"


def _parse_requirements_file(path: Path) -> Dict[str, Requirement]:
    """Parse a requirements.txt into a dict of requirement name -> Requirement."""
    parsed: Dict[str, Requirement] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        req = Requirement(line)
        parsed[req.name.lower()] = req
    return parsed


def _parse_dependency_group(pyproject: Path, group: str) -> Dict[str, Requirement]:
    """Extract a `[dependency-groups]` entry from pyproject.toml without toml parsing."""
    group_pattern = re.compile(
        rf"^{re.escape(group)}\s*=\s*\[(?P<deps>[^\]]*)\]", re.MULTILINE
    )
    match = group_pattern.search(pyproject.read_text(encoding="utf-8"))
    assert match, f"dependency group {group!r} not found in {pyproject}"
    parsed: Dict[str, Requirement] = {}
    for req_str in match.group("deps").split(","):
        req_str = req_str.strip().strip('"').strip("'")
        if req_str:
            req = Requirement(req_str)
            parsed[req.name.lower()] = req
    return parsed


def test_requirements_txt_declares_sqlalchemy() -> None:
    reqs = _parse_requirements_file(SOURCE_REQUIREMENTS)
    assert "sqlalchemy" in reqs, (
        "pg_replication imports dlt.sources.sql_database which requires SQLAlchemy; "
        "declare it in sources/pg_replication/requirements.txt (see #685)"
    )
    assert reqs["sqlalchemy"].specifier, "pin a version constraint for sqlalchemy"


def test_dependency_group_declares_sqlalchemy() -> None:
    reqs = _parse_dependency_group(PYPROJECT, "pg_replication")
    assert "sqlalchemy" in reqs, (
        "pg_replication imports dlt.sources.sql_database which requires SQLAlchemy; "
        "declare it in the pg_replication dependency group of pyproject.toml (see #685)"
    )
