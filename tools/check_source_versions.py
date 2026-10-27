"""Checks that each source changed against the base branch bumps its version and adds a changelog
entry to its README. Sources whose files did not change, or that changed only their README, are
skipped. See `.claude/skills/refresh-source/SKILL.md#source-version-and-readme`.

The base branch is `SOURCES_BASE_REF` if set, `origin/<GITHUB_BASE_REF>` on pull requests,
otherwise `origin/master` or `master`. Run from the repository root:

    uv run python tools/check_source_versions.py
"""

import ast
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Sequence, Set, Tuple

SOURCES_DIR = "sources"
VERSION_FILE = "version.py"
README_FILE = "README.md"
INITIAL_VERSION = "1.0.0"
GUIDE = ".claude/skills/refresh-source/SKILL.md#source-version-and-readme"
VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")

TVersion = Tuple[int, int, int]


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout


def resolve_base(root: Path) -> str:
    """Returns the merge base of HEAD and the base branch"""
    github_base = os.environ.get("GITHUB_BASE_REF")
    refs = [
        os.environ.get("SOURCES_BASE_REF"),
        f"origin/{github_base}" if github_base else None,
        "origin/master",
        "master",
    ]
    for ref in refs:
        if not ref:
            continue
        exists = subprocess.run(
            ["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
            cwd=root,
            capture_output=True,
        )
        if exists.returncode == 0:
            return git(root, "merge-base", ref, "HEAD").strip()
    raise RuntimeError(
        f"No base branch found among {[ref for ref in refs if ref]}, fetch it with"
        " `git fetch origin master` or set SOURCES_BASE_REF"
    )


def changed_paths(root: Path, base: str) -> Set[str]:
    """Paths under `sources` changed since `base`, including uncommitted and untracked files"""
    tracked = git(root, "diff", "--name-only", base, "--", SOURCES_DIR)
    untracked = git(root, "ls-files", "--others", "--exclude-standard", SOURCES_DIR)
    return set(tracked.splitlines()) | set(untracked.splitlines())


def changed_sources(paths: Sequence[str]) -> Set[str]:
    """Names of source folders with changes to files other than their README"""
    names: Set[str] = set()
    for path in paths:
        parts = path.split("/")
        if (
            len(parts) > 2
            and parts[0] == SOURCES_DIR
            and not parts[1].startswith((".", "_"))
            and parts[2:] != [README_FILE]
        ):
            names.add(parts[1])
    return names


def parse_version(code: str) -> TVersion:
    """Returns the `__version__` assigned in `code`.

    Raises ValueError if it is missing or not MAJOR.MINOR.PATCH.
    """
    for node in ast.parse(code).body:
        if not isinstance(node, ast.Assign) or not any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in node.targets
        ):
            continue
        value = node.value.value if isinstance(node.value, ast.Constant) else None
        match = VERSION_PATTERN.match(value) if isinstance(value, str) else None
        if match is None:
            raise ValueError("`__version__` is not a MAJOR.MINOR.PATCH string")
        major, minor, patch = (int(part) for part in match.groups())
        return major, minor, patch
    raise ValueError("there is no `__version__` assignment")


def format_version(version: TVersion) -> str:
    return ".".join(str(part) for part in version)


def has_changelog_entry(readme: str, version: str) -> bool:
    """Tells if `readme` has a heading that starts with `version`, e.g. `### 1.2.0`"""
    pattern = rf"^#+\s+v?{re.escape(version)}(?![\d.])"
    return re.search(pattern, readme, re.MULTILINE) is not None


def base_file(root: Path, base: str, path: str) -> Optional[str]:
    """Returns the content of `path` at `base`, None if it does not exist there"""
    result = subprocess.run(
        ["git", "show", f"{base}:{path}"], cwd=root, capture_output=True, text=True
    )
    return result.stdout if result.returncode == 0 else None


def check_source(root: Path, base: str, name: str, paths: Set[str]) -> List[str]:
    """Returns the problems with the version and the README of a changed source"""
    source_dir = f"{SOURCES_DIR}/{name}"
    if not (root / source_dir / "__init__.py").is_file():
        # the source was removed
        return []
    errors: List[str] = []
    version_path = f"{source_dir}/{VERSION_FILE}"
    version: Optional[TVersion] = None
    if not (root / version_path).is_file():
        errors.append(f'add {version_path} with `__version__ = "{INITIAL_VERSION}"`')
    else:
        try:
            version = parse_version((root / version_path).read_text(encoding="utf-8"))
        except (ValueError, SyntaxError) as ex:
            errors.append(f"{version_path}: {ex}")
    base_code = base_file(root, base, version_path)
    if version and base_code is not None:
        try:
            base_version: Optional[TVersion] = parse_version(base_code)
        except (ValueError, SyntaxError):
            base_version = None
        if base_version and version <= base_version:
            errors.append(
                f"bump the version in {version_path}, it is"
                f" {format_version(base_version)} on the base branch: major for breaking"
                " changes, minor for features, patch for fixes"
            )
    readme_path = f"{source_dir}/{README_FILE}"
    if not (root / readme_path).is_file():
        errors.append(f"add {readme_path} with a changelog")
        return errors
    if readme_path not in paths:
        errors.append(f"update {readme_path}: add a changelog entry for the change")
    elif version and not has_changelog_entry(
        (root / readme_path).read_text(encoding="utf-8"), format_version(version)
    ):
        errors.append(
            f"add a changelog entry `### {format_version(version)}` to {readme_path}"
        )
    return errors


def main(root: Path) -> int:
    base = resolve_base(root)
    paths = changed_paths(root, base)
    sources = sorted(changed_sources(sorted(paths)))
    failed = False
    for name in sources:
        for error in check_source(root, base, name, paths):
            print(f"ERROR: Source {name}: {error}")
            failed = True
    if failed:
        print(f"Changed sources must bump their version and changelog, see {GUIDE}")
        return 1
    print(f"Versions and changelogs of changed sources are up to date: {sources}")
    return 0


if __name__ == "__main__":
    sys.exit(main(Path(git(Path.cwd(), "rev-parse", "--show-toplevel").strip())))
